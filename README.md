# Memcached and Peafowl
Peafowl is an extension of Memcached. It saves power while 
delivering the same performance as Memcached. Please  see the following paper
for more detail:
https://dl.acm.org/doi/abs/10.1145/3419111.3421298


#  Compile and Install
 Please see the Memcached repository for compile and install instructions:
 https://github.com/memcached/memcached
  

#  Run Peafowl
You can run Peafowl using flags that you normally use for Memcached. However, you need 
to run Peafowl with -Q flag  and -t flag  in the privileged mode.

Q flag  pins Peafowl workers to distinct CPU cores.

t flag determines the number of worker threads for Peafowl.

The following command  is the command that we use to run Peafowl:

./memcached -Q -t 10 -u root

