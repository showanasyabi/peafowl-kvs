/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Thread management for memcached.
 */
/*
 * Todo: showan: organize error messages
 */

#include "memcached.h"
#ifdef EXTSTORE
#include "storage.h"
#endif
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>  // peafowl

#ifdef __sun
#include <atomic.h>
#endif

#ifdef TLS
#include <openssl/ssl.h>
#endif

#define ITEMS_PER_ALLOC 64

/* An item in the connection queue. */
enum conn_queue_item_modes {
    queue_new_conn,   /* brand new connection. */
    queue_transfer_conn,  /* Peafowl: transfer a connection to another worker */
    queue_go_home_conn    /* Peafowl: sends a connection  back home (no place is like 127.0.0.1 :) ) */
};
typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item {
    int               sfd;
    enum conn_states  init_state;
    int               event_flags;
    int               read_buffer_size;
    enum network_transport     transport;
    enum conn_queue_item_modes mode;
    conn *c;
    void    *ssl;
    CQ_ITEM          *next;
};

/* A connection queue. */
typedef struct conn_queue CQ;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    pthread_mutex_t lock;
};

/* Locks for cache LRU operations */
pthread_mutex_t lru_locks[POWER_LARGEST];

/* Connection lock around accepting new connections */
pthread_mutex_t conn_lock = PTHREAD_MUTEX_INITIALIZER;

#if !defined(HAVE_GCC_ATOMICS) && !defined(__sun)
pthread_mutex_t atomics_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

/* Lock for global stats */
static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;

/* Lock to cause worker threads to hang up after being woken */
static pthread_mutex_t worker_hang_lock;

/* Free list of CQ_ITEM structs */
static CQ_ITEM *cqi_freelist;
static pthread_mutex_t cqi_freelist_lock;

static pthread_mutex_t *item_locks;
/* size of the item lock hash table */
static uint32_t item_lock_count;
unsigned int item_lock_hashpower;
#define hashsize(n) ((unsigned long int)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
static LIBEVENT_THREAD *threads;

/*
 * Number of worker threads that have finished setting themselves up.
 */
static int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;


static void thread_libevent_process(evutil_socket_t fd, short which, void *arg);

extern void event_handler(const int fd, const short which, void *arg); // showan : we get this from memcached.c

/* item_lock() must be held for an item before any modifications to either its
 * associated hash bucket, or the structure itself.
 * LRU modifications must hold the item lock, and the LRU lock.
 * LRU's accessing items must item_trylock() before modifying an item.
 * Items accessible from an LRU must not be freed or modified
 * without first locking and removing from the LRU.
 */


void item_lock(uint32_t hv) {
    mutex_lock(&item_locks[hv & hashmask(item_lock_hashpower)]);
}

void *item_trylock(uint32_t hv) {
    pthread_mutex_t *lock = &item_locks[hv & hashmask(item_lock_hashpower)];
    if (pthread_mutex_trylock(lock) == 0) {
        return lock;
    }
    return NULL;
}

void item_trylock_unlock(void *lock) {
    mutex_unlock((pthread_mutex_t *) lock);
}

void item_unlock(uint32_t hv) {
    mutex_unlock(&item_locks[hv & hashmask(item_lock_hashpower)]);
}

static void wait_for_thread_registration(int nthreads) {
    while (init_count < nthreads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }
}

static void register_thread_initialized(void) {
    pthread_mutex_lock(&init_lock);
    init_count++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);
    /* Force worker threads to pile up if someone wants us to */
    pthread_mutex_lock(&worker_hang_lock);
    pthread_mutex_unlock(&worker_hang_lock);
}

/* Must not be called with any deeper locks held */
void pause_threads(enum pause_thread_types type) {
    char buf[1];
    int i;

    buf[0] = 0;
    switch (type) {
        case PAUSE_ALL_THREADS:
            slabs_rebalancer_pause();
            lru_maintainer_pause();
            lru_crawler_pause();
#ifdef EXTSTORE
            storage_compact_pause();
            storage_write_pause();
#endif
        case PAUSE_WORKER_THREADS:
            buf[0] = 'p';
            pthread_mutex_lock(&worker_hang_lock);
            break;
        case RESUME_ALL_THREADS:
            slabs_rebalancer_resume();
            lru_maintainer_resume();
            lru_crawler_resume();
#ifdef EXTSTORE
            storage_compact_resume();
            storage_write_resume();
#endif
        case RESUME_WORKER_THREADS:
            pthread_mutex_unlock(&worker_hang_lock);
            break;
        default:
            fprintf(stderr, "Unknown lock type: %d\n", type);
            assert(1 == 0);
            break;
    }

    /* Only send a message if we have one. */
    if (buf[0] == 0) {
        return;
    }

    pthread_mutex_lock(&init_lock);
    init_count = 0;
    for (i = 0; i < settings.num_threads; i++) {
        if (write(threads[i].notify_send_fd, buf, 1) != 1) {
            perror("Failed writing to notify pipe");
            /* TODO: This is a fatal problem. Can it ever happen temporarily? */
        }
    }
    wait_for_thread_registration(settings.num_threads);
    pthread_mutex_unlock(&init_lock);
}

// MUST not be called with any deeper locks held
// MUST be called only by parent thread
// Note: listener thread is the "main" event base, which has exited its
// loop in order to call this function.
void stop_threads(void) {
    char buf[1];
    int i;

    // assoc can call pause_threads(), so we have to stop it first.
    stop_assoc_maintenance_thread();
    if (settings.verbose > 0)
        fprintf(stderr, "stopped assoc\n");

    if (settings.verbose > 0)
        fprintf(stderr, "asking workers to stop\n");
    buf[0] = 's';
    pthread_mutex_lock(&worker_hang_lock);
    pthread_mutex_lock(&init_lock);
    init_count = 0;
    for (i = 0; i < settings.num_threads; i++) {
        if (write(threads[i].notify_send_fd, buf, 1) != 1) {
            perror("Failed writing to notify pipe");
            /* TODO: This is a fatal problem. Can it ever happen temporarily? */
        }
    }
    wait_for_thread_registration(settings.num_threads);
    pthread_mutex_unlock(&init_lock);

    // All of the workers are hung but haven't done cleanup yet.

    if (settings.verbose > 0)
        fprintf(stderr, "asking background threads to stop\n");

    // stop each side thread.
    // TODO: Verify these all work if the threads are already stopped
    stop_item_crawler_thread(CRAWLER_WAIT);
    if (settings.verbose > 0)
        fprintf(stderr, "stopped lru crawler\n");
    if (settings.lru_maintainer_thread) {
        stop_lru_maintainer_thread();
        if (settings.verbose > 0)
            fprintf(stderr, "stopped maintainer\n");
    }
    if (settings.slab_reassign) {
        stop_slab_maintenance_thread();
        if (settings.verbose > 0)
            fprintf(stderr, "stopped slab mover\n");
    }
    logger_stop();
    if (settings.verbose > 0)
        fprintf(stderr, "stopped logger thread\n");
    stop_conn_timeout_thread();
    if (settings.verbose > 0)
        fprintf(stderr, "stopped idle timeout thread\n");

    // Close all connections then let the workers finally exit.
    if (settings.verbose > 0)
        fprintf(stderr, "closing connections\n");
    conn_close_all();
    pthread_mutex_unlock(&worker_hang_lock);
    if (settings.verbose > 0)
        fprintf(stderr, "reaping worker threads\n");
    for (i = 0; i < settings.num_threads; i++) {
        pthread_join(threads[i].thread_id, NULL);
    }

    if (settings.verbose > 0)
        fprintf(stderr, "all background threads stopped\n");

    // At this point, every background thread must be stopped.
}

/*
 * Initializes a connection queue.
 */
static void cq_init(CQ *cq) {
    pthread_mutex_init(&cq->lock, NULL);
    cq->head = NULL;
    cq->tail = NULL;
}

/*
 * Looks for an item on a connection queue, but doesn't block if there isn't
 * one.
 * Returns the item, or NULL if no item is available
 */
static CQ_ITEM *cq_pop(CQ *cq) {
    CQ_ITEM *item;

    pthread_mutex_lock(&cq->lock);
    item = cq->head;
    if (NULL != item) {
        cq->head = item->next;
        if (NULL == cq->head)
            cq->tail = NULL;
    }
    pthread_mutex_unlock(&cq->lock);

    return item;
}

/*
 * Adds an item to a connection queue.
 */
static void cq_push(CQ *cq, CQ_ITEM *item) {
    item->next = NULL;

    pthread_mutex_lock(&cq->lock);
    if (NULL == cq->tail)
        cq->head = item;
    else
        cq->tail->next = item;
    cq->tail = item;
    pthread_mutex_unlock(&cq->lock);
}

/*
 * Returns a fresh connection queue item.
 */
static CQ_ITEM *cqi_new(void) {
    CQ_ITEM *item = NULL;
    pthread_mutex_lock(&cqi_freelist_lock);
    if (cqi_freelist) {
        item = cqi_freelist;
        cqi_freelist = item->next;
    }
    pthread_mutex_unlock(&cqi_freelist_lock);

    if (NULL == item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        item = malloc(sizeof(CQ_ITEM) * ITEMS_PER_ALLOC);
        if (NULL == item) {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            return NULL;
        }

        /*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < ITEMS_PER_ALLOC; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&cqi_freelist_lock);
        item[ITEMS_PER_ALLOC - 1].next = cqi_freelist;
        cqi_freelist = &item[1];
        pthread_mutex_unlock(&cqi_freelist_lock);
    }

    return item;
}


/*
 * Frees a connection queue item (adds it to the freelist.)
 */
static void cqi_free(CQ_ITEM *item) {
    pthread_mutex_lock(&cqi_freelist_lock);
    item->next = cqi_freelist;
    cqi_freelist = item;
    pthread_mutex_unlock(&cqi_freelist_lock);
}


/*
 * Creates a worker thread.
 */
static void create_worker(void *(*func)(void *), void *arg) {
    pthread_attr_t  attr;
    int             ret;

    pthread_attr_init(&attr);

    /*  affine threads/workers to distinct CPU cores */
    if (settings.thread_affinity)
    {
        const int offset = settings.thread_affinity_offset;
        static int current_cpu = 0;

        static int max_cpus = 8 * sizeof(cpu_set_t);
        cpu_set_t m;
        int i = 0;

        CPU_ZERO(&m);
        sched_getaffinity(0, sizeof(cpu_set_t), &m);
        int id = ((LIBEVENT_THREAD *)arg)->index;
        for (i = 0; i < max_cpus; i++)
        {
            int c = (current_cpu + i + 1) % (max_cpus);
            if (c < offset)
                c += offset;
            if (CPU_ISSET(c, &m))
            {
                CPU_ZERO(&m);
                CPU_SET(c, &m);

                if ((ret = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &m)) != 0)
                {
                    fprintf(stderr, "Can't set thread affinity: %s\n",
                            strerror(ret));
                    exit(1);
                }

                if (settings.verbose > 0)
                    fprintf(stderr, "setting thread %d to cpu %d\n", id, c);


                current_cpu = c;
                break;
            }
        }
    }

    if ((ret = pthread_create(&((LIBEVENT_THREAD*)arg)->thread_id, &attr, func, arg)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n",
                strerror(ret));
        exit(1);
    }
}

/*
 * Sets whether or not we accept new connections.
 */
void accept_new_conns(const bool do_accept) {
    pthread_mutex_lock(&conn_lock);
    do_accept_new_conns(do_accept);
    pthread_mutex_unlock(&conn_lock);
}

/**
 *  Here if peafowl gets a rest command from the administrator
 *  it resets all it has learned so far about workers
 */
void peafowl_reset_sched_stats(void){
    for (int i = 0; i < settings.num_threads; i++) {
        threads[i].ignored_time_window = 300;
        threads[i].current_load = 0;
        threads[i].peak_load = 0;
        threads[i].capacity = 0;
        threads[i].resident_load = 0;
    }
}



/**
 * peafowl scheduler. It gets signals for worker threads to update the load
 * based on the load and capacity of the workers, peafowl determines the scale-down and the scale-up workers
 * @param fd
 * @param which
 * @param arg
 */
static void peafowl_libevent_process(evutil_socket_t fd, short which, void *arg) {
    char buf[1];
    //conn *c;

    if (read(fd, buf, 1) != 1) {
        if (settings.verbose > 0)
            fprintf(stderr, "Can't read from libevent pipe\n");
        return;
    }
    switch (buf[0]) {
        case 'u':
            /* a connection is being transferred. No scheduling happens until the transfer is done */
            if(peafowl.transferring_epoch == true) {
                return;
            }

            if (peafowl.update_scale_down_worker) {
                /* find  the scale_down worker ( the worker with the lowest load) */
                for (int i = 0; i < settings.num_threads; i++) {
                    if (threads[i].active  &&  threads[i].idle_state_enabled) {
                        cpuidle_states_disable(i+1,1);
                        threads[i].idle_state_enabled = false;
                    }
                    if (threads[i].active && threads[i].ignored_time_window < 1) {
                        if (threads[i].current_load < threads[peafowl.scale_down_worker].current_load || !threads[peafowl.scale_down_worker].active || threads[peafowl.scale_down_worker].ignored_time_window > 0) {
                            peafowl.scale_down_worker = i;
                        }
                    }
                }
            }

            /* find the destination worker  and total available capacity */
            double total_capacity = 0;
            for(int j = 0; j < settings.num_threads; j++) {
                if (threads[j].active && j != peafowl.scale_down_worker  && threads[j].ignored_time_window < 1) {
                    total_capacity += threads[j].capacity;
                    if (threads[j].capacity >  threads[peafowl.destination_worker].capacity || peafowl.destination_worker ==  peafowl.scale_down_worker) {
                        peafowl.destination_worker = j;
                    }
                }
            }

            if( peafowl.destination_worker ==  peafowl.scale_down_worker) {
                return;
            }
            if (!threads[peafowl.destination_worker].active){
                return;
            }
            if (threads[peafowl.destination_worker].ignored_time_window > 0) {
                return;
            }
            if (!threads[peafowl.scale_down_worker].active){
                return;
            }
            if (threads[peafowl.scale_down_worker].ignored_time_window > 0) {
                return;
            }

            if (threads[peafowl.scale_down_worker].current_load < total_capacity) {
                /* we wont update the scale-down worker until the current scale_down worker gives up all of its connection */
                if (peafowl.update_scale_down_worker) {
                    peafowl.update_scale_down_worker = false;
                }
                // start a transferring epoch over which,the scheduler locks the scheduling process
                peafowl.transferring_epoch = true;
                // instruct the scale-down worker to gives up a connection
                threads[peafowl.scale_down_worker].instructed_send_conn = true;

            }


            /* when the transfer is done. The destination worker sends  signal "d" to the scheduler*/
            break;
        case 'd':
            peafowl.transferring_epoch = false;

    }

}


/****************************** LIBEVENT THREADS *****************************/

/*
 * Set up a thread's information.
 */
static void setup_thread(LIBEVENT_THREAD *me) {
#if defined(LIBEVENT_VERSION_NUMBER) && LIBEVENT_VERSION_NUMBER >= 0x02000101
    struct event_config *ev_config;
    ev_config = event_config_new();
    event_config_set_flag(ev_config, EVENT_BASE_FLAG_NOLOCK);
    me->base = event_base_new_with_config(ev_config);
    event_config_free(ev_config);
#else
    me->base = event_init();
#endif

    if (! me->base) {
        fprintf(stderr, "Can't allocate event base\n");
        exit(1);
    }

    /* Listen for notifications from other threads */
    event_set(&me->notify_event, me->notify_receive_fd,
              EV_READ | EV_PERSIST, thread_libevent_process, me);
    event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) {
        fprintf(stderr, "Can't monitor libevent notify pipe\n");
        exit(1);
    }

    /* Peafowl: making communication channel for workers and the peafowl scheduler */
    event_set(&me->peafowl_sched_event, me->reciv_peafowl_msg_fd,
              EV_READ | EV_PERSIST, peafowl_libevent_process, me);
    event_base_set(main_base, &me->peafowl_sched_event);

    if (event_add(&me->peafowl_sched_event, 0) == -1) {
        fprintf(stderr, "Can't monitor peafowl scheduler notify pipe\n");
        exit(1);
    }

    me->new_conn_queue = malloc(sizeof(struct conn_queue));
    if (me->new_conn_queue == NULL) {
        perror("Failed to allocate memory for connection queue");
        exit(EXIT_FAILURE);
    }
    cq_init(me->new_conn_queue);

    if (pthread_mutex_init(&me->stats.mutex, NULL) != 0) {
        perror("Failed to initialize mutex");
        exit(EXIT_FAILURE);
    }

    me->rbuf_cache = cache_create("rbuf", READ_BUFFER_SIZE, sizeof(char *), NULL, NULL);
    if (me->rbuf_cache == NULL) {
        fprintf(stderr, "Failed to create read buffer cache\n");
        exit(EXIT_FAILURE);
    }
    // Note: we were cleanly passing in num_threads before, but this now
    // relies on settings globals too much.
    if (settings.read_buf_mem_limit) {
        int limit = settings.read_buf_mem_limit / settings.num_threads;
        if (limit < READ_BUFFER_SIZE) {
            limit = 1;
        } else {
            limit = limit / READ_BUFFER_SIZE;
        }
        cache_set_limit(me->rbuf_cache, limit);
    }

#ifdef EXTSTORE
    me->io_cache = cache_create("io", sizeof(io_wrap), sizeof(char*), NULL, NULL);
    if (me->io_cache == NULL) {
        fprintf(stderr, "Failed to create IO object cache\n");
        exit(EXIT_FAILURE);
    }
#endif
#ifdef TLS
    if (settings.ssl_enabled) {
        me->ssl_wbuf = (char *)malloc((size_t)settings.ssl_wbuf_size);
        if (me->ssl_wbuf == NULL) {
            fprintf(stderr, "Failed to allocate the SSL write buffer\n");
            exit(EXIT_FAILURE);
        }
    }
#endif
}

/*
 * Worker thread: main event loop
 */
static void *worker_libevent(void *arg) {
    LIBEVENT_THREAD *me = arg;

    /* Any per-thread setup can happen here; memcached_thread_init() will block until
     * all threads have finished initializing.
     */
    me->l = logger_create();
    me->lru_bump_buf = item_lru_bump_buf_create();
    if (me->l == NULL || me->lru_bump_buf == NULL) {
        abort();
    }

    if (settings.drop_privileges) {
        drop_worker_privileges();
    }

    register_thread_initialized();

    event_base_loop(me->base, 0);

    // same mechanism used to watch for all threads exiting.
    register_thread_initialized();

    event_base_free(me->base);
    return NULL;
}



void transfer_connection(conn *c) {
    if(c->state == conn_closed || c->state == conn_closing) {
        return;
    }

    if(event_del(&c->event) == -1) {
        printf("connection event_set cannot be deleted");
        return;
    }
    LIBEVENT_THREAD *thread = threads + peafowl.destination_worker;
    c->thread = thread;

    if(c->thread->index != c->home) {
        c->is_guest = true;
    } else {
        c->is_guest = false;
    }

    CQ_ITEM  *item= cqi_new();
    if(item == NULL) {
        printf("Error: cant make a new connection queue item");
        c->state = conn_closed;
        close(c->sfd);
        return;
    }

    char buf[1];
    item->c = c;
    item->init_state = conn_new_cmd;
    item->mode = queue_transfer_conn;
    cq_push(thread->new_conn_queue, item);
    buf[0] = 'c';
    if(write(thread->notify_send_fd, buf , 1) != 1) {
        printf("Transfer Error- cant signal the destination worker");
    }
}


void send_conn_home(conn *c) {
    if(c->state == conn_closed || c->state == conn_closing) {
        return;
    }

    if(event_del(&c->event) == -1) {
        printf("connection event_set cannot be deleted");
        return;
    }
    LIBEVENT_THREAD *thread = threads + c->home;
    c->thread = thread;
    c->is_guest = false;

    CQ_ITEM  *item= cqi_new();
    if(item == NULL) {
        printf("Error: cant make a new connection queue item");
        c->state = conn_closed;
        close(c->sfd);
        return;
    }

    char buf[1];
    item->c = c;
    item->init_state = conn_new_cmd;
    item->mode = queue_go_home_conn;
    cq_push(thread->new_conn_queue, item);
    buf[0] = 'c';
    if(write(thread->notify_send_fd, buf , 1) != 1) {
        printf("Transfer Error- cant signal the destination worker");
    }
}





/*
 * Processes an incoming "handle a new connection" item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
static void thread_libevent_process(evutil_socket_t fd, short which, void *arg) {
    LIBEVENT_THREAD *me = arg;
    CQ_ITEM *item;
    char buf[1];
    conn *c;
    unsigned int fd_from_pipe;

    if (read(fd, buf, 1) != 1) {
        if (settings.verbose > 0)
            fprintf(stderr, "Can't read from libevent pipe\n");
        return;
    }

    switch (buf[0]) {
    case 'c':
        item = cq_pop(me->new_conn_queue);

        if (NULL == item) {
            break;
        }
        switch (item->mode) {
            case queue_new_conn:
                c = conn_new(item->sfd, item->init_state, item->event_flags,
                                   item->read_buffer_size, item->transport,
                                   me->base, item->ssl);
                if (c == NULL) {
                    if (IS_UDP(item->transport)) {
                        fprintf(stderr, "Can't listen for events on UDP socket\n");
                        exit(1);
                    } else {
                        if (settings.verbose > 0) {
                            fprintf(stderr, "Can't listen for events on fd %d\n",
                                item->sfd);
                        }
#ifdef TLS
                        if (item->ssl) {
                            SSL_shutdown(item->ssl);
                            SSL_free(item->ssl);
                        }
#endif
                        close(item->sfd);
                    }
                } else {
                    c->thread = me;
                    /**
                     * peafowl
                     */
                    me->num_active_conn ++;
                    c->home = me->index;
                    if(!me->active) {
                        me->active = true;
                        peafowl.num_active_workers++;
                    }

#ifdef TLS
                    if (settings.ssl_enabled && c->ssl != NULL) {
                        assert(c->thread && c->thread->ssl_wbuf);
                        c->ssl_wbuf = c->thread->ssl_wbuf;
                    }
#endif
                }
                break;

            case queue_transfer_conn: // peafowl
                c = item->c;
                if (c == NULL) {
                    if (IS_UDP(item->transport)) {
                        fprintf(stderr, "Can't listen for events on UDP socket\n");
                        exit(1);
                    } else {
                        if (settings.verbose > 0) {
                            fprintf(stderr, "Can't listen for events on fd %d\n",
                                    item->sfd);
                        }
                        close(item->sfd);
                    }
                } else {
                    c->thread = me;
                    c->thread->current_load += c->rate;
                    c->on_load = true;
                    c->thread->capacity = c->thread->peak_load - c->thread->current_load;
                    c->thread->num_active_conn ++;
                    if(!c->thread->active) {
                        c->thread->active = true;
                    }

                    // set the libevent for the connection
                    c->ev_flags = EV_READ | EV_PERSIST;
                    //printf ("^^^^^ IN attacker: %d ---  accepting conn %d , conn state: %d  ",me->index, c->sfd, c->state);
                    event_set(&c->event, c->sfd, c->ev_flags, event_handler, (void *)c);
                    event_base_set(c->thread->base, &c->event);
                    if (event_add(&c->event, 0) == -1)
                    {
                        printf(" peafowl: Error event_add \n \n \n");
                    }

                    //send a signal to scheduler letting him/her know that the transfer is done
                    char msg[1];
                    msg[0] = 'd';
                    if (write(c->thread->send_peafowl_msg_fd, msg, 1) != 1) {
                        printf("Error: Cant send message to peafowl scheduler");
                    }

                }
                break;

            case queue_go_home_conn: // peafowl
                c = item->c;
                if (c == NULL) {
                    if (IS_UDP(item->transport)) {
                        fprintf(stderr, "Can't listen for events on UDP socket\n");
                        exit(1);
                    } else {
                        if (settings.verbose > 0) {
                            fprintf(stderr, "Can't listen for events on fd %d\n",
                                    item->sfd);
                        }
                        close(item->sfd);
                    }
                } else {
                    c->thread = me;
                    c->thread->current_load += c->rate;
                    c->on_load = true;
                    c->thread->resident_load += c->rate;
                    if(c->thread->resident_load > c->thread->peak_load) {
                        c->thread->peak_load = c->thread->resident_load;
                    }
                    c->thread->capacity = c->thread->peak_load - c->thread->current_load;
                    c->thread->num_active_conn ++;
                    if(!c->thread->active) {
                        c->thread->active = true;
                    }

                    //This worker just got one of his/her connections back home
                    //Lets ignore this thread for a while for scheduling
                    c->thread->ignored_time_window = 30;

                    // set the libevent for the connection
                    c->ev_flags = EV_READ | EV_PERSIST;
                    event_set(&c->event, c->sfd, c->ev_flags, event_handler, (void *)c);
                    event_base_set(c->thread->base, &c->event);
                    if (event_add(&c->event, 0) == -1)
                    {
                        printf("Error : Error event_add \n \n \n");
                    }
                }
                break;
        }
        cqi_free(item);
        break;
    /* we were told to pause and report in */
    case 'p':
        register_thread_initialized();
        break;
    /* a client socket timed out */
    case 't':
        if (read(fd, &fd_from_pipe, sizeof(fd_from_pipe)) != sizeof(fd_from_pipe)) {
            if (settings.verbose > 0)
                fprintf(stderr, "Can't read timeout fd from libevent pipe\n");
            return;
        }
        conn_close_idle(conns[fd_from_pipe]);
        break;
    /* a side thread redispatched a client connection */
    case 'r':
        if (read(fd, &fd_from_pipe, sizeof(fd_from_pipe)) != sizeof(fd_from_pipe)) {
            if (settings.verbose > 0)
                fprintf(stderr, "Can't read redispatch fd from libevent pipe\n");
            return;
        }
        conn_worker_readd(conns[fd_from_pipe]);
        break;
    /* asked to stop */
    case 's':
        event_base_loopexit(me->base, NULL);
        break;
    }
}

/* Which thread we assigned a connection to most recently. */
static int last_thread = -1;

/*
 * Dispatches a new connection to another thread. This is only ever called
 * from the main thread, either during initialization (for UDP) or because
 * of an incoming connection.
 */
void dispatch_conn_new(int sfd, enum conn_states init_state, int event_flags,
                       int read_buffer_size, enum network_transport transport, void *ssl) {
    CQ_ITEM *item = cqi_new();
    char buf[1];
    if (item == NULL) {
        close(sfd);
        /* given that malloc failed this may also fail, but let's try */
        fprintf(stderr, "Failed to allocate memory for connection object\n");
        return;
    }

    int tid = (last_thread + 1) % settings.num_threads;

    LIBEVENT_THREAD *thread = threads + tid;

    last_thread = tid;

    item->sfd = sfd;
    item->init_state = init_state;
    item->event_flags = event_flags;
    item->read_buffer_size = read_buffer_size;
    item->transport = transport;
    item->mode = queue_new_conn;
    item->ssl = ssl;

    cq_push(thread->new_conn_queue, item);

    MEMCACHED_CONN_DISPATCH(sfd, (int64_t)thread->thread_id);
    buf[0] = 'c';
    if (write(thread->notify_send_fd, buf, 1) != 1) {
        perror("Writing to thread notify pipe");
    }
}

/*
 * Re-dispatches a connection back to the original thread. Can be called from
 * any side thread borrowing a connection.
 */
#define REDISPATCH_MSG_SIZE (1 + sizeof(int))
void redispatch_conn(conn *c) {
    char buf[REDISPATCH_MSG_SIZE];
    LIBEVENT_THREAD *thread = c->thread;

    buf[0] = 'r';
    memcpy(&buf[1], &c->sfd, sizeof(int));
    if (write(thread->notify_send_fd, buf, REDISPATCH_MSG_SIZE) != REDISPATCH_MSG_SIZE) {
        perror("Writing redispatch to thread notify pipe");
    }
}

/* This misses the allow_new_conns flag :( */
void sidethread_conn_close(conn *c) {
    if (settings.verbose > 1)
        fprintf(stderr, "<%d connection closing from side thread.\n", c->sfd);

    c->state = conn_closing;
    // redispatch will see closing flag and properly close connection.
    redispatch_conn(c);
    return;
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
item *item_alloc(char *key, size_t nkey, int flags, rel_time_t exptime, int nbytes) {
    item *it;
    /* do_item_alloc handles its own locks */
    it = do_item_alloc(key, nkey, flags, exptime, nbytes);
    return it;
}

/*
 * Returns an item if it hasn't been marked as expired,
 * lazy-expiring as needed.
 */
item *item_get(const char *key, const size_t nkey, conn *c, const bool do_update) {
    item *it;
    uint32_t hv;
    hv = hash(key, nkey);
    item_lock(hv);
    it = do_item_get(key, nkey, hv, c, do_update);
    item_unlock(hv);
    return it;
}

// returns an item with the item lock held.
// lock will still be held even if return is NULL, allowing caller to replace
// an item atomically if desired.
item *item_get_locked(const char *key, const size_t nkey, conn *c, const bool do_update, uint32_t *hv) {
    item *it;
    *hv = hash(key, nkey);
    item_lock(*hv);
    it = do_item_get(key, nkey, *hv, c, do_update);
    return it;
}

item *item_touch(const char *key, size_t nkey, uint32_t exptime, conn *c) {
    item *it;
    uint32_t hv;
    hv = hash(key, nkey);
    item_lock(hv);
    it = do_item_touch(key, nkey, exptime, hv, c);
    item_unlock(hv);
    return it;
}

/*
 * Links an item into the LRU and hashtable.
 */
int item_link(item *item) {
    int ret;
    uint32_t hv;

    hv = hash(ITEM_key(item), item->nkey);
    item_lock(hv);
    ret = do_item_link(item, hv);
    item_unlock(hv);
    return ret;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_remove(item *item) {
    uint32_t hv;
    hv = hash(ITEM_key(item), item->nkey);

    item_lock(hv);
    do_item_remove(item);
    item_unlock(hv);
}

/*
 * Replaces one item with another in the hashtable.
 * Unprotected by a mutex lock since the core server does not require
 * it to be thread-safe.
 */
int item_replace(item *old_it, item *new_it, const uint32_t hv) {
    return do_item_replace(old_it, new_it, hv);
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void item_unlink(item *item) {
    uint32_t hv;
    hv = hash(ITEM_key(item), item->nkey);
    item_lock(hv);
    do_item_unlink(item, hv);
    item_unlock(hv);
}

/*
 * Does arithmetic on a numeric item value.
 */
enum delta_result_type add_delta(conn *c, const char *key,
                                 const size_t nkey, bool incr,
                                 const int64_t delta, char *buf,
                                 uint64_t *cas) {
    enum delta_result_type ret;
    uint32_t hv;

    hv = hash(key, nkey);
    item_lock(hv);
    ret = do_add_delta(c, key, nkey, incr, delta, buf, cas, hv, NULL);
    item_unlock(hv);
    return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
enum store_item_type store_item(item *item, int comm, conn* c) {
    enum store_item_type ret;
    uint32_t hv;

    hv = hash(ITEM_key(item), item->nkey);
    item_lock(hv);
    ret = do_store_item(item, comm, c, hv);
    item_unlock(hv);
    return ret;
}

/******************************* GLOBAL STATS ******************************/

void STATS_LOCK() {
    pthread_mutex_lock(&stats_lock);
}

void STATS_UNLOCK() {
    pthread_mutex_unlock(&stats_lock);
}

void threadlocal_stats_reset(void) {
    int ii;
    for (ii = 0; ii < settings.num_threads; ++ii) {
        pthread_mutex_lock(&threads[ii].stats.mutex);
#define X(name) threads[ii].stats.name = 0;
        THREAD_STATS_FIELDS
#ifdef EXTSTORE
        EXTSTORE_THREAD_STATS_FIELDS
#endif
#undef X

        memset(&threads[ii].stats.slab_stats, 0,
                sizeof(threads[ii].stats.slab_stats));
        memset(&threads[ii].stats.lru_hits, 0,
                sizeof(uint64_t) * POWER_LARGEST);

        pthread_mutex_unlock(&threads[ii].stats.mutex);
    }
}

void threadlocal_stats_aggregate(struct thread_stats *stats) {
    int ii, sid;

    /* The struct has a mutex, but we can safely set the whole thing
     * to zero since it is unused when aggregating. */
    memset(stats, 0, sizeof(*stats));

    for (ii = 0; ii < settings.num_threads; ++ii) {
        pthread_mutex_lock(&threads[ii].stats.mutex);
#define X(name) stats->name += threads[ii].stats.name;
        THREAD_STATS_FIELDS
#ifdef EXTSTORE
        EXTSTORE_THREAD_STATS_FIELDS
#endif
#undef X

        for (sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
#define X(name) stats->slab_stats[sid].name += \
            threads[ii].stats.slab_stats[sid].name;
            SLAB_STATS_FIELDS
#undef X
        }

        for (sid = 0; sid < POWER_LARGEST; sid++) {
            stats->lru_hits[sid] +=
                threads[ii].stats.lru_hits[sid];
            stats->slab_stats[CLEAR_LRU(sid)].get_hits +=
                threads[ii].stats.lru_hits[sid];
        }

        stats->read_buf_count += threads[ii].rbuf_cache->total;
        stats->read_buf_bytes += threads[ii].rbuf_cache->total * READ_BUFFER_SIZE;
        stats->read_buf_bytes_free += threads[ii].rbuf_cache->freecurr * READ_BUFFER_SIZE;
        pthread_mutex_unlock(&threads[ii].stats.mutex);
    }
}

void slab_stats_aggregate(struct thread_stats *stats, struct slab_stats *out) {
    int sid;

    memset(out, 0, sizeof(*out));

    for (sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
#define X(name) out->name += stats->slab_stats[sid].name;
        SLAB_STATS_FIELDS
#undef X
    }
}

/*
 * Initializes the thread subsystem, creating various worker threads.
 *
 * nthreads  Number of worker event handler threads to spawn
 */
void memcached_thread_init(int nthreads, void *arg) {
    int         i;
    int         power;

    for (i = 0; i < POWER_LARGEST; i++) {
        pthread_mutex_init(&lru_locks[i], NULL);
    }
    pthread_mutex_init(&worker_hang_lock, NULL);

    pthread_mutex_init(&init_lock, NULL);
    pthread_cond_init(&init_cond, NULL);

    pthread_mutex_init(&cqi_freelist_lock, NULL);
    cqi_freelist = NULL;

    /* Want a wide lock table, but don't waste memory */
    if (nthreads < 3) {
        power = 10;
    } else if (nthreads < 4) {
        power = 11;
    } else if (nthreads < 5) {
        power = 12;
    } else if (nthreads <= 10) {
        power = 13;
    } else if (nthreads <= 20) {
        power = 14;
    } else {
        /* 32k buckets. just under the hashpower default. */
        power = 15;
    }

    if (power >= hashpower) {
        fprintf(stderr, "Hash table power size (%d) cannot be equal to or less than item lock table (%d)\n", hashpower, power);
        fprintf(stderr, "Item lock table grows with `-t N` (worker threadcount)\n");
        fprintf(stderr, "Hash table grows with `-o hashpower=N` \n");
        exit(1);
    }

    item_lock_count = hashsize(power);
    item_lock_hashpower = power;

    item_locks = calloc(item_lock_count, sizeof(pthread_mutex_t));
    if (! item_locks) {
        perror("Can't allocate item locks");
        exit(1);
    }
    for (i = 0; i < item_lock_count; i++) {
        pthread_mutex_init(&item_locks[i], NULL);
    }

    threads = calloc(nthreads, sizeof(LIBEVENT_THREAD));
    if (! threads) {
        perror("Can't allocate thread descriptors");
        exit(1);
    }

    for (i = 0; i < nthreads; i++) {
        int fds[2];
        if (pipe(fds)) {
            perror("Can't create notify pipe");
            exit(1);
        }

        threads[i].notify_receive_fd = fds[0];
        threads[i].notify_send_fd = fds[1];


        // setup communication channel for peafowl scheduler and the workers
        int peafowl_fd[2];
        if(pipe(peafowl_fd)) {
            printf(" Peafowl Error: creating  communication channels");
        }

        threads[i].send_peafowl_msg_fd = peafowl_fd[1];
        threads[i].reciv_peafowl_msg_fd = peafowl_fd[0];


#ifdef EXTSTORE
        threads[i].storage = arg;
#endif
        setup_thread(&threads[i]);
        /* Reserve three fds for the libevent base, and two for the pipe */
        stats_state.reserved_fds += 5;
        /**
         * Initialize peafowl related thread/worker members
         */
         threads[i].current_load = 0;
         threads[i].resident_load = 0;
         threads[i].peak_load = 0;
         threads[i].capacity = 0;
         threads[i].num_active_conn = 0;
         threads[i].index = i;
         threads[i].last_signaling_time = current_time;
         threads[i].active = true;
         threads[i].idle_state_enabled = true;
         threads[i].ignored_time_window = 0;
         threads[i].instructed_send_conn = false;
    }
    /**
     *  Initialize peafowl setting
     *
     */
    peafowl.monitoring_period = 1;
    peafowl.num_active_workers = nthreads;
    peafowl.scale_down_worker = 0;
    peafowl.destination_worker = 0;
    peafowl.transferring_epoch = false;
    peafowl.update_scale_down_worker = true;

    /* Create threads after we've done all the libevent setup. */
    for (i = 0; i < nthreads; i++) {
        create_worker(worker_libevent, &threads[i]);
    }

    /* Wait for all the threads to set themselves up before returning. */
    pthread_mutex_lock(&init_lock);
    wait_for_thread_registration(nthreads);
    pthread_mutex_unlock(&init_lock);
}

