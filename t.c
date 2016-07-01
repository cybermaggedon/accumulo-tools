
#include <stdio.h>
#include "accumulo_comms.h"
#include <stdlib.h>
#include <time.h>

int main(int argc, char** argv)
{

    accumulo_comms* comms;

    comms = accumulo_connect("localhost4", 42424, "root", "accumulo");

    if (comms == 0) {
	fprintf(stderr, "Failed to connect.\n");
	exit(1);
    }

    accumulo_writer* wr = accumulo_writer_create(comms, "mytest");

    uint64_t now = time(0) * 1000;

    accumulo_writer_add_write(wr, "lion:eats", "spo", "zebra", "", now, "zebra");
    accumulo_writer_add_write(wr, "eats:zebra", "pos", "lion", "", now, "lion");
    accumulo_writer_add_write(wr, "zebra:lion", "osp", "eats", "", now, "eats");

    accumulo_writer_flush(wr);

    accumulo_writer_free(wr);

    // --------

    accumulo_query* qry = accumulo_query_create(comms, "mytest");

    accumulo_query_set_range(qry, "a", "zzz");
    //    accumulo_query_set_colf(qry, "spo");
    accumulo_iterator* it = accumulo_query_execute(qry);

    while (accumulo_iterator_has_next(it)) {

      accumulo_kv* kv = accumulo_iterator_get_next(it);
      printf("Row: %s\n", kv->rowid);
      printf("Value: %s\n", kv->value);
      printf("\n");

    }

    accumulo_iterator_free(it);

    accumulo_query_free(qry);

    // --------

    accumulo_disconnect(comms);

    exit(0);

}
