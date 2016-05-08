## For students: change this path to the root of your code
CODEROOT = "../"

#CC = gcc 
CC = g++  -std=c++11 

#CPPFLAGS = -Wall -I$(CODEROOT) -O3  # maximal optimization
CPPFLAGS = -std=c++11 -w -I$(CODEROOT) -g     # with debugging info
