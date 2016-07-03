
#include <stdint.h>

//#include <accumulo_query.h>

#ifdef __cplusplus
extern "C" {
#endif
  
struct accumulo_comms_str;
typedef struct accumulo_comms_str accumulo_comms;

struct accumulo_writer_str;
typedef struct accumulo_writer_str accumulo_writer;

accumulo_comms* accumulo_connect(const char* host, unsigned int port,
				 const char* user, const char* password);
void accumulo_disconnect(accumulo_comms*);

accumulo_writer* accumulo_writer_create(accumulo_comms*, const char* table);
void accumulo_writer_add_write(accumulo_writer* obj, const char* row,
			       const char* colf, const char* colq,
			       const char* vis, uint64_t timestamp,
			       const char* val);

int accumulo_writer_flush(accumulo_writer*);
  void accumulo_writer_free(accumulo_writer*);

struct accumulo_query_str;
typedef struct accumulo_query_str accumulo_query;

struct accumulo_iterator_str;
typedef struct accumulo_iterator_str accumulo_iterator;

  accumulo_query* accumulo_query_create(accumulo_comms*, const char* table);
int accumulo_query_set_range(accumulo_query*, const char* s, const char* e);
int accumulo_query_set_colf(accumulo_query*,const char*);
int accumulo_query_set_col(accumulo_query*,const char*, const char*);
accumulo_iterator* accumulo_query_execute(accumulo_query*);

  void accumulo_query_free(accumulo_query*);

  void accumulo_iterator_free(accumulo_iterator*);

  int accumulo_iterator_has_next(accumulo_iterator*);

  typedef struct {
    const char* rowid;
    const char* colf;
    const char* colq;
    const char* colv;
    uint64_t timestamp;
    const char* value;
  } accumulo_kv;

  accumulo_kv* accumulo_iterator_get_next(accumulo_iterator*);
  
#ifdef __cplusplus
}
#endif

/*

int accumulo_add(accumulo_comms* gc, const char*, const char*, const char*);

accumulo_results* accumulo_find(accumulo_comms*, const char* path,
				accumulo_query*);

void accumulo_results_free(accumulo_results* results);

*/
