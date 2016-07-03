
#include <accumulo_comms.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <AccumuloAPI.h>

struct accumulo_comms_str {
    Connector* conn;
    Authorizations* auth;
};

accumulo_comms* accumulo_connect(const char* host, unsigned int port,
				 const char* user, const char* pass)
{
    accumulo_comms* ac = new accumulo_comms;
    ac->conn = new Connector(host, port, user, pass);
    ac->auth = new Authorizations("");

    try {
	ac->conn->tableOperations().createTable("mytest");
    } catch (TableExistsException& e) {
	std::cerr << "Table exists error ignored." << std::endl;
    }

    return ac;
}


void accumulo_disconnect(accumulo_comms* ac)
{
    ac->conn->close();
    delete ac->conn;
    delete ac->auth;
    delete ac;
}

struct accumulo_writer_str {
    accumulo_writer_str(BatchWriter& wr) : writer(wr) {}
    BatchWriter writer;
};

accumulo_writer* accumulo_writer_create(accumulo_comms* ac, const char* table)
{
  //    BatchWriter w = ac->conn->createBatchWriter(table, 5000, 10000, 10000, 2);
    BatchWriter w = ac->conn->createBatchWriter(table, 500, 1000, 1000, 2);
    accumulo_writer* wr = new accumulo_writer(w);
    return wr;
}

void accumulo_writer_free(accumulo_writer* wr)
{
    wr->writer.close();
    delete wr;
}

void accumulo_writer_add_write(accumulo_writer* obj, const char* row,
			       const char* colf, const char* colq,
			       const char* vis, uint64_t timestamp,
			       const char* val)
{
    Mutation mut(row);
    mut.put(colf, colq, vis, timestamp, val);
    obj->writer.addMutation(mut);
}

int accumulo_writer_flush(accumulo_writer* wr)
{

    wr->writer.flush();
    
}

struct accumulo_query_str {
    accumulo_query_str(const Scanner& s) : s(s) {}
    Scanner s;
    KeyValue kv;
};

accumulo_query* accumulo_query_create(accumulo_comms* ac, const char* table)
{
    std::string t(table);
    Scanner s = ac->conn->createScanner(t, *(ac->auth));
    return new accumulo_query_str(s);
}

int accumulo_query_set_range(accumulo_query* q, const char* s, const char* e)
{
    Key ks, ke;
    ks.row = s;
    ke.row = e;
    Range r;
    r.start = ks;
    r.stop = ke;
    q->s.setRange(r);
}

int accumulo_query_set_colf(accumulo_query* q,const char* colf)
{
    q->s.fetchColumnFamily(std::string(colf));
}

int accumulo_query_set_col(accumulo_query* q, const char* colf,
			   const char* colq)
{
    q->s.fetchColumn(std::string(colf), std::string(colq));
}

struct accumulo_iterator_str {
    accumulo_iterator_str(const ScannerIterator& it) : it(it) {}
    ScannerIterator it;
    KeyValue kv;
    accumulo_kv row;
};


int accumulo_iterator_has_next(accumulo_iterator* i)
{
    bool has_next = i->it.hasNext();

    if (has_next) {
	i->kv = i->it.next();
	
	i->row.rowid = i->kv.key.row.c_str();
	i->row.colf = i->kv.key.colFamily.c_str();
	i->row.colq = i->kv.key.colQualifier.c_str();
	i->row.colv = i->kv.key.colVisibility.c_str();
	i->row.timestamp = i->kv.key.timestamp;
	i->row.value = i->kv.value.c_str();

	return 1;
    }
    return 0;
}

accumulo_kv* accumulo_iterator_get_next(accumulo_iterator* i)
{
    return &(i->row);
}

accumulo_iterator* accumulo_query_execute(accumulo_query* q)
{
    ScannerIterator it = q->s.iterator();


    return new accumulo_iterator(it);
}

void accumulo_query_free(accumulo_query* q) {
    delete q;
}

void accumulo_iterator_free(accumulo_iterator* it) {
    it->it.close();
    delete it;
}

