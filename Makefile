#

all: tpq


tpq:	tpq.h tpq.c
	gcc -Wall -o tpq tpq.c
