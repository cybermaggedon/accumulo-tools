/* -*- Mode: c; c-basic-offset: 2 -*-
 *
 * rdf_storage_accumulo.c - RDF Storage using Accumulo implementation
 *
 * Copyright (C) 2004-2010, David Beckett http://www.dajobe.org/
 * Copyright (C) 2004-2005, University of Bristol, UK http://www.bristol.ac.uk/
 * 
 * This package is Free Software and part of Redland http://librdf.org/
 * 
 * It is licensed under the following three licenses as alternatives:
 *   1. GNU Lesser General Public License (LGPL) V2.1 or any newer version
 *   2. GNU General Public License (GPL) V2 or any newer version
 *   3. Apache License, V2.0 or any newer version
 * 
 * You may not use this file except in compliance with at least one of
 * the above three licenses.
 * 
 * See LICENSE.html or LICENSE.txt at the top of this package for the
 * complete terms and further detail along with the license texts for
 * the licenses in COPYING.LIB, COPYING and LICENSE-2.0.txt respectively.
 * 
 * 
 */


#ifdef HAVE_CONFIG_H
#include <rdf_config.h>
#endif

#ifdef WIN32
#include <win32_rdf_config.h>
#endif

#include <stdio.h>
#include <string.h>
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <sys/types.h>

#include <redland.h>
#include <rdf_storage.h>
#include <rdf_heuristics.h>

#include <accumulo_comms.h>

typedef struct
{
    librdf_storage *storage;

    int is_new;
  
    char *name;
    size_t name_len;

    char* host;
    unsigned int port;

    accumulo_comms* comms;

} librdf_storage_accumulo_instance;

typedef enum { SPO, POS, OSP } index_type;

/* prototypes for local functions */
static int librdf_storage_accumulo_init(librdf_storage* storage, const char *name, librdf_hash* options);
static int librdf_storage_accumulo_open(librdf_storage* storage, librdf_model* model);
static int librdf_storage_accumulo_close(librdf_storage* storage);
static int librdf_storage_accumulo_size(librdf_storage* storage);
static int librdf_storage_accumulo_add_statement(librdf_storage* storage, librdf_statement* statement);
static int librdf_storage_accumulo_add_statements(librdf_storage* storage, librdf_stream* statement_stream);
static int librdf_storage_accumulo_remove_statement(librdf_storage* storage, librdf_statement* statement);
static int librdf_storage_accumulo_contains_statement(librdf_storage* storage, librdf_statement* statement);
static librdf_stream* librdf_storage_accumulo_serialise(librdf_storage* storage);
static librdf_stream* librdf_storage_accumulo_find_statements(librdf_storage* storage, librdf_statement* statement);

/* serialising implementing functions */
static int accumulo_results_stream_end_of_stream(void* context);
static int accumulo_results_stream_next_statement(void* context);
static void* accumulo_results_stream_get_statement(void* context, int flags);
static void accumulo_results_stream_finished(void* context);

/* context functions */
static int librdf_storage_accumulo_context_add_statement(librdf_storage* storage, librdf_node* context_node, librdf_statement* statement);
static int librdf_storage_accumulo_context_remove_statement(librdf_storage* storage, librdf_node* context_node, librdf_statement* statement);
static int librdf_storage_accumulo_context_contains_statement(librdf_storage* storage, librdf_node* context, librdf_statement* statement);
static librdf_stream* librdf_storage_accumulo_context_serialise(librdf_storage* storage, librdf_node* context_node);

/* helper functions for contexts */

static librdf_iterator* librdf_storage_accumulo_get_contexts(librdf_storage* storage);

/* transactions */
static int librdf_storage_accumulo_transaction_start(librdf_storage *storage);
static int librdf_storage_accumulo_transaction_commit(librdf_storage *storage);
static int librdf_storage_accumulo_transaction_rollback(librdf_storage *storage);

static void librdf_storage_accumulo_register_factory(librdf_storage_factory *factory);
#ifdef MODULAR_LIBRDF
void librdf_storage_module_register_factory(librdf_world *world);
#endif


/* functions implementing storage api */
static int
librdf_storage_accumulo_init(librdf_storage* storage, const char *name,
                           librdf_hash* options)
{

    char *name_copy;
    librdf_storage_accumulo_instance* context;
  
    if(!name) {
	if(options)
	    librdf_free_hash(options);
	return 1;
    }
  
    context = LIBRDF_CALLOC(librdf_storage_accumulo_instance*, 1,
			    sizeof(*context));
    if(!context) {
	if(options)
	    librdf_free_hash(options);
	return 1;
    }

    librdf_storage_set_instance(storage, context);
  
    context->storage = storage;
    context->name_len = strlen(name);
//    context->transaction = 0;

    name_copy = LIBRDF_MALLOC(char*, context->name_len + 1);
    if(!name_copy) {
	if(options)
	    librdf_free_hash(options);
	return 1;
    }

    strcpy(name_copy, name);
    context->name = name_copy;

    char* pos = strchr(name, ':');

    if (pos == 0) {
	context->host = strdup(name);
	context->port = 42424;
    } else {
	context->host = strndup(name, pos - name);
	context->port = atoi(pos + 1);
    }

    // Add options here.

    /* no more options, might as well free them now */
    if(options)
	librdf_free_hash(options);

    // FIXME: Hard-coded;
    context->comms = accumulo_connect(context->host, context->port,
				      "root", "accumulo");
    if (context->comms == 0) {
	free(context->name);
	free(context);
	return 1;
    }

    return 0;

}


static void
librdf_storage_accumulo_terminate(librdf_storage* storage)
{
    librdf_storage_accumulo_instance* context;

    context = (librdf_storage_accumulo_instance*)storage->instance;
  
    if (context == NULL)
	return;

    if(context->name)
	LIBRDF_FREE(char*, context->name);
  
    LIBRDF_FREE(librdf_storage_accumulo_terminate, storage->instance);
}

static
char* node_helper(librdf_storage* storage, librdf_node* node, char node_type)
{

    librdf_uri* uri;
    librdf_uri* dt_uri;

    const char* integer_type = "http://www.w3.org/2001/XMLSchema#integer";
    const char* float_type = "http://www.w3.org/2001/XMLSchema#float";
    const char* datetime_type = "http://www.w3.org/2001/XMLSchema#dateTime";

    char* name;
    char data_type;

    switch(librdf_node_get_type(node)) {

    case LIBRDF_NODE_TYPE_RESOURCE:
	uri = librdf_node_get_uri(node);
	name = librdf_uri_as_string(uri);
	data_type = 'u';
	break;
	
    case LIBRDF_NODE_TYPE_LITERAL:
	dt_uri = librdf_node_get_literal_value_datatype_uri(node);
	if (dt_uri == 0)
	    data_type = 's';
	else {
	    const char* type_uri = librdf_uri_as_string(dt_uri);
	    if (strcmp(type_uri, integer_type) == 0)
		data_type = 'i';
	    else if (strcmp(type_uri, float_type) == 0)
		data_type = 'f';
	    else if (strcmp(type_uri, datetime_type) == 0)
		data_type = 'd';
	    else
		data_type = 's';
	}
	name = librdf_node_get_literal_value(node);
	break;

    case LIBRDF_NODE_TYPE_BLANK:
	name = librdf_node_get_blank_identifier(node);
	data_type = 'b';
	break;

    case LIBRDF_NODE_TYPE_UNKNOWN:
	break;
	
    }

    char* term = malloc(5 + strlen(name));
    if (term == 0) {
	fprintf(stderr, "malloc failed");
	return 0;
    }
    
    sprintf(term, "%c:%c:%s", node_type, data_type, name);

    return term;

}


static int
statement_helper(librdf_storage* storage,
		 librdf_statement* statement,
		 librdf_node* context,
		 char** s, char** p, char** o, char** c)
{

    librdf_node* sn = librdf_statement_get_subject(statement);
    librdf_node* pn = librdf_statement_get_predicate(statement);
    librdf_node* on = librdf_statement_get_object(statement);

    if (sn)
	*s = node_helper(storage, sn, 'n');
    else
	*s = 0;
    
    if (pn)
	*p = node_helper(storage, pn, 'r');
    else
	*p = 0;

    if (on)
	*o = node_helper(storage, on, 'n');
    else
	*o = 0;

    if (context)
	*c = node_helper(storage, context, 'c');
    else
	*c = 0;

}


static accumulo_query* accumulo_query_(accumulo_comms* c,
				       const char* s, const char* p,
				       const char* o, index_type* tp)
{

    accumulo_query* q = accumulo_query_create(
#ifdef BROKEN
    *spo = 0;
    *filter = 1;
    *path = "graph/doOperation";
    accumulo_query* qry = accumulo_create_query();
    accumulo_configure_range_query(qry, "n:", "n;");
    return qry;
#endif
}

static accumulo_query* accumulo_query_s(accumulo_comms* c,
					const char* s, const char* p,
					const char* o, index_type* tp)
{
#ifdef ASD
    *spo = 0;
    *filter = 1;
    *path = "graph/doOperation/get/edges/related";
    accumulo_query* qry = accumulo_create_query();
    accumulo_configure_relationship_filter_view(qry);
    accumulo_configure_entity_seed(qry, s);
    json_object_object_add(qry, "includeIncomingOutGoing",
			   json_object_new_string("OUTGOING"));
    return qry;
#endif
}

static accumulo_query* accumulo_query_p(accumulo_comms* c,
					const char* s, const char* p,
					const char* o, index_type* tp)
{
#ifdef BROKEN
    *spo = 1;
    *filter = 1;
    *path = "graph/doOperation/get/edges/related";
    accumulo_query* qry = accumulo_create_query();
    accumulo_configure_entity_seed(qry, p);
    json_object_object_add(qry, "includeIncomingOutGoing",
			   json_object_new_string("INCOMING"));
    return qry;
#endif
}

static accumulo_query* accumulo_query_o(accumulo_comms* c,
					const char* s, const char* p,
					const char* o, index_type* tp)
{
#ifdef BROKEN
    *spo = 0;
    *filter = 1;
    *path = "graph/doOperation/get/edges/related";
    accumulo_query* qry = accumulo_create_query();
    accumulo_configure_entity_seed(qry, o);
    json_object_object_add(qry, "includeIncomingOutGoing",
			   json_object_new_string("INCOMING"));
    return qry;
#endif
}

static accumulo_query* accumulo_query_sp(accumulo_comms* c,
					 const char* s, const char* p,
					 const char* o, index_type* tp)
{
#ifdef BROKEN
    *spo = 1;
    *filter = 1;
    *path = "graph/doOperation/get/edges/related";
    accumulo_query* qry = accumulo_create_query();
    accumulo_configure_edge_seeds(qry, s, p);
    json_object_object_add(qry, "includeIncomingOutGoing",
			   json_object_new_string("INCOMING"));
    return qry;
#endif
}

static accumulo_query* accumulo_query_so(accumulo_comms* c,
					 const char* s, const char* p,
					 const char* o, index_type* tp)
{
#ifdef BROKEN
    *spo = 1;
    *filter = 1;
    *path = "graph/doOperation/get/edges/related";
    accumulo_query* qry = accumulo_create_query();
    accumulo_configure_entity_seed(qry, s);
    accumulo_configure_edge_filter_view(qry, o);
    json_object_object_add(qry, "includeIncomingOutGoing",
			   json_object_new_string("OUTGOING"));
    return qry;
#endif
}

static accumulo_query* accumulo_query_po(accumulo_comms* c,
					 const char* s, const char* p,
					 const char* o, index_type* tp)
{
#ifdef BROKEN
    *spo = 0;
   *filter = 1;
    *path = "graph/doOperation/get/edges/related";
    accumulo_query* qry = accumulo_create_query();
    accumulo_configure_entity_seed(qry, o);
    accumulo_configure_edge_filter_view(qry, p);
    json_object_object_add(qry, "includeIncomingOutGoing",
			   json_object_new_string("INCOMING"));
    return qry;
#endif
}

static accumulo_query* accumulo_query_spo(accumulo_comms* c,
					  const char* s, const char* p,
					  const char* o, index_type* tp)
{
#ifdef BROKEN
    *spo = 0;
    *filter = 1;
    *path = "graph/doOperation/get/edges/related";
    accumulo_query* qry = accumulo_create_query();
    accumulo_configure_edge_seeds(qry, s, o);
    accumulo_configure_edge_filter_view(qry, p);
    return qry;
#endif
}
  
static int
librdf_storage_accumulo_open(librdf_storage* storage, librdf_model* model)
{
    librdf_storage_accumulo_instance* context;

    context = (librdf_storage_accumulo_instance*)storage->instance;

    return 0;
}


/**
 * librdf_storage_accumulo_close:
 * @storage: the storage
 *
 * Close the accumulo storage.
 * 
 * Return value: non 0 on failure
 **/
static int
librdf_storage_accumulo_close(librdf_storage* storage)
{
#ifdef BROKEN
    librdf_storage_accumulo_instance* context;
    context = (librdf_storage_accumulo_instance*)storage->instance;

    if (context->comms) {
	accumulo_disconnect(context->comms);
	context->comms = 0;
    }

    if (context->transaction)
	accumulo_elements_free(context->transaction);

    return 0;
#endif
}

static int
librdf_storage_accumulo_size(librdf_storage* storage)
{
#ifdef BROKEN

    librdf_storage_accumulo_instance* context =
	(librdf_storage_accumulo_instance*) storage->instance;

    int are_spo;
    int filter;
    char* path;

    accumulo_query* qry = accumulo_query_(0, 0, 0, &are_spo, &filter, &path);

    accumulo_results* res = accumulo_find(context->comms, path, qry);
    if (res == 0) {
	accumulo_query_free(qry);
	fprintf(stderr, "Query execute failed.\n");
	exit(1);
    }

    accumulo_results_iterator* iter = accumulo_iterator_create(res);

    accumulo_query_free(qry);

    int count = 0;
    
    while (!accumulo_iterator_done(iter)) {
	const char* a, * b, * c;
	int val;
    
	accumulo_iterator_get(iter, &a, &b, &c, &val);

	if ((val > 0) && (b[0] != '@') && (c[0] != '@'))
	    count++;
	accumulo_iterator_next(iter);
    }

    accumulo_iterator_free(iter);

    accumulo_results_free(res);

    return count;
#endif

}

static int
librdf_storage_accumulo_add_statement(librdf_storage* storage, 
                                    librdf_statement* statement)
{
    return librdf_storage_accumulo_context_add_statement(storage, NULL, statement);
}


static int
librdf_storage_accumulo_add_statements(librdf_storage* storage,
                                     librdf_stream* statement_stream)
{

  return 0;
#ifdef BROKEN

    librdf_storage_accumulo_instance* context;
    context = (librdf_storage_accumulo_instance*)storage->instance;

    accumulo_elements* elts;
    elts = accumulo_elements_create();

    const int batch_size = 1000;
    int rows = 0;

    for(; !librdf_stream_end(statement_stream);
	librdf_stream_next(statement_stream)) {

	librdf_statement* statement;
	librdf_node* context_node;
    
	statement = librdf_stream_get_object(statement_stream);
	context_node = librdf_stream_get_context2(statement_stream);

	if(!statement) {
	    break;
	}

	char* s;
	char* p;
	char* o;
	char* c;
	statement_helper(storage, statement, context_node, &s, &p, &o, &c);

	if (p[0] == '@') continue;

	/* Create S,O -> P */
	accumulo_add_edge_object(elts, p, s, o, "@r", 1);

	/* Create S,P -> O */
	accumulo_add_edge_object(elts, o, s, p, "@n", 1);

	if (s) free(s);
	if (p) free(p);
	if (o) free(o);

	if (rows++ > batch_size) {

	    int ret = accumulo_add_elements(context->comms, elts);
	    accumulo_elements_free(elts);
	    elts = accumulo_elements_create();

	    rows = 0;

	}

    }

    if (rows > 0) {
	int ret = accumulo_add_elements(context->comms, elts);
    }

    accumulo_elements_free(elts);
    
    return 0;
#endif

}


static int
librdf_storage_accumulo_remove_statement(librdf_storage* storage,
                                       librdf_statement* statement)
{
    return librdf_storage_accumulo_context_remove_statement(storage, NULL, 
							  statement);
}

static int
librdf_storage_accumulo_contains_statement(librdf_storage* storage, 
                                         librdf_statement* statement)
{
    return librdf_storage_accumulo_context_contains_statement(storage, NULL,
							    statement);
}


static int
librdf_storage_accumulo_context_contains_statement(librdf_storage* storage,
                                                 librdf_node* context_node,
                                                 librdf_statement* statement)
{

#ifdef BROKEN
    librdf_storage_accumulo_instance* context;
    context = (librdf_storage_accumulo_instance*)storage->instance;

    /* librdf_storage_accumulo_instance* context; */
    accumulo_term terms[4];

    statement_helper(storage, statement, terms, context_node);

    int count = accumulo_count(context->comms, terms[0], terms[1], terms[2]);
    if (count < 0)
	return -1;
    
    return (count > 0);
#endif
}

typedef struct {
    
    librdf_storage *storage;
    librdf_storage_accumulo_instance* accumulo_context;

    librdf_statement *statement;
    librdf_node* context;

    accumulo_iterator* it;
    index_type tp;

} accumulo_results_stream;

static
librdf_node* node_constructor_helper(librdf_world* world, const char* t)
{

#ifdef BROKEN
    librdf_node* o;

    if ((strlen(t) < 4) || (t[1] != ':') || (t[3] != ':')) {
	fprintf(stderr, "node_constructor_helper called on invalid term\n");
	return 0;
    }

    if (t[2] == 'u') {
 	o = librdf_new_node_from_uri_string(world,
					    (unsigned char*) t + 4);
	return o;
    }

    if (t[2] == 's') {
	o = librdf_new_node_from_literal(world,
					 (unsigned char*) t + 4, 0, 0);
	return o;
    }


    if (t[2] == 'i') {
	librdf_uri* dt =
	    librdf_new_uri(world,
			   "http://www.w3.org/2001/XMLSchema#integer");
	if (dt == 0)
	    return 0;

	o = librdf_new_node_from_typed_literal(world, t + 4, 0, dt);
	librdf_free_uri(dt);
	return o;
    }
    
    if (t[2] == 'f') {
	librdf_uri* dt =
	    librdf_new_uri(world,
			   "http://www.w3.org/2001/XMLSchema#float");
	if (dt == 0)
	    return 0;

	o = librdf_new_node_from_typed_literal(world, t + 4, 0, dt);
	librdf_free_uri(dt);
	return o;
    }

    if (t[2] == 'd') {
	librdf_uri* dt =
	    librdf_new_uri(world,
			   "http://www.w3.org/2001/XMLSchema#dateTime");
	if (dt == 0)
	    return 0;

	o = librdf_new_node_from_typed_literal(world, t + 4, 0, dt);
	librdf_free_uri(dt);
	return o;
    }    

    return librdf_new_node_from_literal(world,
					(unsigned char*) t + 4, 0, 0);

#endif
}

static int
accumulo_results_stream_end_of_stream(void* context)
{
#ifdef BROKEN
    accumulo_results_stream* scontext;

    scontext = (accumulo_results_stream*)context;

    accumulo_results_iterator* iter = scontext->iterator;

    if (accumulo_iterator_done(iter))
	return 1;

    return 0;
#endif

}


static int
accumulo_results_stream_next_statement(void* context)
{
#ifdef BROKEN
    accumulo_results_stream* scontext;

    scontext = (accumulo_results_stream*)context;

    accumulo_iterator_next(scontext->iterator);

    while (!accumulo_iterator_done(scontext->iterator)) {

	const char* a, *b, *c;
	int val;

	accumulo_iterator_get(scontext->iterator, &a, &b, &c, &val);

	if ((val < 1) || (a[0] == '@') || (b[0] == '@') || (c[0] == '@')) {
	    accumulo_iterator_next(scontext->iterator);
	    continue;
	}

	break;
	    
    }

    return 0;
#endif

}


static void*
accumulo_results_stream_get_statement(void* context, int flags)
{

#ifdef BROKEN
    accumulo_results_stream* scontext;
    const char* a;
    const char* b;
    const char* c;
	
    scontext = (accumulo_results_stream*)context;

    accumulo_results_iterator* iter = scontext->iterator;

    switch(flags) {

	int val;

    case LIBRDF_ITERATOR_GET_METHOD_GET_OBJECT:

	accumulo_iterator_get(iter, &a, &b, &c, &val);

	if (scontext->statement) {
	    librdf_free_statement(scontext->statement);
	    scontext->statement = 0;
	}

	librdf_node* sn, * pn, * on;
	sn = node_constructor_helper(scontext->storage->world, a);

	if (scontext->are_spo) {
	    pn = node_constructor_helper(scontext->storage->world, b);
	    on = node_constructor_helper(scontext->storage->world, c);

	} else {
	    pn = node_constructor_helper(scontext->storage->world, c);
	    on = node_constructor_helper(scontext->storage->world, b);
	}

	if (sn == 0 || pn == 0 || on == 0) {
	    if (sn) librdf_free_node(sn);
	    if (pn) librdf_free_node(pn);
	    if (on) librdf_free_node(on);
	    return 0;
	}

	scontext->statement =
	    librdf_new_statement_from_nodes(scontext->storage->world,
					    sn, pn, on);

	return scontext->statement;

    case LIBRDF_ITERATOR_GET_METHOD_GET_CONTEXT:
	return scontext->context;

    default:
	librdf_log(scontext->storage->world,
		   0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
		   "Unknown iterator method flag %d", flags);
	return NULL;
    }

#endif
}

static void
accumulo_results_stream_finished(void* context)
{

#ifdef BROKEN
    accumulo_results_stream* scontext;

    scontext  = (accumulo_results_stream*)context;

    if (scontext->iterator) {
	accumulo_iterator_free(scontext->iterator);
	scontext->iterator = 0;
    }

    if (scontext->results) {
	accumulo_results_free(scontext->results);
	scontext->results = 0;
    }
	
    if(scontext->storage)
	librdf_storage_remove_reference(scontext->storage);

    if(scontext->statement)
	librdf_free_statement(scontext->statement);

    if(scontext->context)
	librdf_free_node(scontext->context);

    LIBRDF_FREE(librdf_storage_accumulo_find_statements_stream_context, scontext);

#endif
}

static librdf_stream*
librdf_storage_accumulo_serialise(librdf_storage* storage)
{

#ifdef BROKEN

    librdf_storage_accumulo_instance* context =
	(librdf_storage_accumulo_instance*) storage->instance;
    
    context = (librdf_storage_accumulo_instance*)storage->instance;

    accumulo_results_stream* scontext;
    
    scontext =
	LIBRDF_CALLOC(accumulo_results_stream*, 1, sizeof(*scontext));
    if(!scontext)
	return NULL;

    scontext->storage = storage;
    librdf_storage_add_reference(scontext->storage);

    int are_spo;
    int filter_spo;
    char* path;

    accumulo_query* qry = accumulo_query_(0, 0, 0, &are_spo, &filter_spo, &path);

    accumulo_results* results = accumulo_find(context->comms, path, qry);

    accumulo_query_free(qry);
    
    if (results == 0) {
	fprintf(stderr, "Query execute failed.\n");
	exit(1);
    }

    scontext->results = results;
    scontext->iterator = accumulo_iterator_create(results);
    scontext->are_spo = are_spo;
    scontext->filter_spo = filter_spo;

    while (!accumulo_iterator_done(scontext->iterator)) {

	const char* a, *b, *c;
	int val;

	accumulo_iterator_get(scontext->iterator, &a, &b, &c, &val);

	if ((val < 1) || (a[0] == '@') || (b[0] == '@') || (c[0] == '@')) {
	    accumulo_iterator_next(scontext->iterator);
	    continue;
	}

	break;
	    
    }

    librdf_stream* stream;
    stream =
	librdf_new_stream(storage->world,
			  (void*)scontext,
			  &accumulo_results_stream_end_of_stream,
			  &accumulo_results_stream_next_statement,
			  &accumulo_results_stream_get_statement,
			  &accumulo_results_stream_finished);
    if(!stream) {
	accumulo_results_stream_finished((void*)scontext);
	return NULL;
    }
  
    return stream;
#endif

}


/**
 * librdf_storage_accumulo_find_statements:
 * @storage: the storage
 * @statement: the statement to match
 *
 * .
 * 
 * Return a stream of statements matching the given statement (or
 * all statements if NULL).  Parts (subject, predicate, object) of the
 * statement can be empty in which case any statement part will match that.
 * Uses #librdf_statement_match to do the matching.
 * 
 * Return value: a #librdf_stream or NULL on failure
 **/
static librdf_stream*
librdf_storage_accumulo_find_statements(librdf_storage* storage,
					librdf_statement* statement)
{

    librdf_storage_accumulo_instance* context;
    accumulo_results_stream* scontext;
    librdf_stream* stream;
    char* s;
    char* p;
    char* o;
    char* c;
    
    context = (librdf_storage_accumulo_instance*)storage->instance;

    scontext =
	LIBRDF_CALLOC(accumulo_results_stream*, 1, sizeof(*scontext));
    if(!scontext)
	return NULL;

    scontext->storage = storage;
    librdf_storage_add_reference(scontext->storage);

    scontext->accumulo_context = context;

    statement_helper(storage, statement, 0, &s, &p, &o, &c);

    int index = 0;

    int are_spo = 0;
    
    typedef accumulo_query* (*query_function)(accumulo_comms*, const char* s,
					      const char* p, const char* o,
					      index_type* type);

    query_function functions[8] = {
	&accumulo_query_,		/* ??? */
	&accumulo_query_s,	/* S?? */
	&accumulo_query_p,	/* ?P? */
	&accumulo_query_sp,	/* SP? */
	&accumulo_query_o,	/* ??O */
	&accumulo_query_so,	/* S?O */
	&accumulo_query_po,	/* ?PO */
	&accumulo_query_spo	/* SPO */
    };

    /* This creates an index into the function table, depending on input
       terms. */
    int num = 0;
    if (o) num += 4;
    if (p) num += 2;
    if (s) num++;

    index_type tp;
    
    query_function fn = functions[num];
    accumulo_query* query = (*fn)(instance->comms,
				  (const char*) s,
				  (const char*) p,
				  (const char*) o,
				  &tp);

    printf("ASD\n");
    accumulo_iterator* it = accumulo_query_execute(query);
    printf("DEF\n");

    accumulo_query_free(query);

    if (it == 0) {
	fprintf(stderr, "Failed to execute query.\n");
        return 0;
    }

    scontext->it = it;

    /*
    while (!accumulo_iterator_done(scontext->iterator)) {

	const char* a, *b, *c;
	int val;

	accumulo_iterator_get(scontext->iterator, &a, &b, &c, &val);

	if ((val < 1) || (b[0] == '@') || (c[0] == '@')) {
	    accumulo_iterator_next(scontext->iterator);
	    continue;
	}

	break;
	    
    }
    */

    stream =
	librdf_new_stream(storage->world,
			  (void*)scontext,
			  &accumulo_results_stream_end_of_stream,
			  &accumulo_results_stream_next_statement,
			  &accumulo_results_stream_get_statement,
			  &accumulo_results_stream_finished);
    if(!stream) {
	accumulo_results_stream_finished((void*)scontext);
	return NULL;
    }
  
    return stream;

}

/**
 * librdf_storage_accumulo_context_add_statement:
 * @storage: #librdf_storage object
 * @context_node: #librdf_node object
 * @statement: #librdf_statement statement to add
 *
 * Add a statement to a storage context.
 * 
 * Return value: non 0 on failure
 **/
static int
librdf_storage_accumulo_context_add_statement(librdf_storage* storage,
                                            librdf_node* context_node,
                                            librdf_statement* statement) 
{

    char* s;
    char* p;
    char* o;
    char* c;

    statement_helper(storage, statement, context_node, &s, &p, &o, &c);

    librdf_storage_accumulo_instance* context; 
    context = (librdf_storage_accumulo_instance*)storage->instance;

    printf("%s %s %s\n", s, p, o);

    accumulo_writer* wr = accumulo_writer_create(context->comms, "mytest");

    uint64_t now = time(0) * 1000;

    accumulo_writer_add_write(wr, s, "spo", p, "", now, o);
    accumulo_writer_add_write(wr, p, "pos", o, "", now, s);
    accumulo_writer_add_write(wr, o, "osp", s, "", now, p);

    accumulo_writer_flush(wr);
    accumulo_writer_free(wr);

#ifdef ASDASDSD
    if (context->transaction) {

	/* Create S,O -> P */
	accumulo_add_edge_object(context->transaction, p, s, o, "@r", 1);

	/* Create S,P -> O */
	accumulo_add_edge_object(context->transaction, o, s, p, "@n", 1);

	if (s) free(s);
	if (p) free(p);
	if (o) free(o);

	return 0;

    }

    accumulo_elements* elts = accumulo_elements_create(context->comms);

    /* Create S,O -> P */
    accumulo_add_edge_object(elts, p, s, o, "@r", 1);

    /* Create S,P -> O */
    accumulo_add_edge_object(elts, o, s, p, "@n", 1);

    if (s) free(s);
    if (p) free(p);
    if (o) free(o);

    int ret = accumulo_add_elements(context->comms, elts);

    accumulo_elements_free(elts);

    if (ret < 0)
	return -1;

    return 0;
#endif

}


/**
 * librdf_storage_accumulo_context_remove_statement:
 * @storage: #librdf_storage object
 * @context_node: #librdf_node object
 * @statement: #librdf_statement statement to remove
 *
 * Remove a statement from a storage context.
 * 
 * Return value: non 0 on failure
 **/
static int
librdf_storage_accumulo_context_remove_statement(librdf_storage* storage, 
                                               librdf_node* context_node,
                                               librdf_statement* statement) 
{

#ifdef BROKEN
    librdf_storage_accumulo_instance* context; 
    context = (librdf_storage_accumulo_instance*)storage->instance;

    char* s;
    char* p;
    char* o;
    char* c;

    statement_helper(storage, statement, context_node, &s, &p, &o, &c);

    int are_spo, filter;
    char* path;
    
    accumulo_query* qry = accumulo_query_spo(s, p, o, &are_spo, &filter, &path);

    accumulo_results* res = accumulo_find(context->comms, path, qry);
    if (res == 0) {
        free(s); free(p); free(o); free(c);
	accumulo_query_free(qry);
	fprintf(stderr, "Query execute failed.\n");
	exit(1);
    }

    accumulo_query_free(qry);

    if (json_object_array_length(res) < 1) {
	free(s); free(p); free(o); free(c);
	accumulo_results_free(res);
	return -1;
    }

    json_object* obj = json_object_array_get_idx(res, 0);
    if (obj == 0) {
	free(s); free(p); free(o); free(c);
	accumulo_results_free(res);
	return -1;
    }

    if (!json_object_object_get_ex(obj, "properties", &obj)) {
	free(s); free(p); free(o); free(c);
	accumulo_results_free(res);
	return -1;
    }

    if (!json_object_object_get_ex(obj, "name", &obj)) {
	free(s); free(p); free(o); free(c);
	accumulo_results_free(res);
	return -1;
    }

    if (!json_object_object_get_ex(obj,
				   "accumulo.function.simple.types.FreqMap",
				   &obj)) {
	free(s); free(p); free(o); free(c);
	accumulo_results_free(res);
	return -1;
    }
      
    if (!json_object_object_get_ex(obj, p, &obj)) {
	free(s); free(p); free(o); free(c);
	accumulo_results_free(res);
	return -1;
    }

    /* This is the value in the freq map. */
    int weight = json_object_get_int(obj);

    accumulo_results_free(res);

    accumulo_elements* elts = accumulo_elements_create();

    /* Create S,O -> P */
    accumulo_add_edge_object(elts, p, s, o, "@r", -weight);

    /* Create S,P -> O */
    accumulo_add_edge_object(elts, o, s, p, "@n", -weight);

    free(s); free(p); free(o); free(c);

    int ret = accumulo_add_elements(context->comms, elts);

    accumulo_elements_free(elts);

    if (ret < 0)
	return -1;

    return 0;

#endif
}


static  int
librdf_storage_accumulo_context_remove_statements(librdf_storage* storage, 
                                                librdf_node* context_node)
{

    //FIXME: Not implemented.

    return -1;

}

/**
 * librdf_storage_accumulo_context_serialise:
 * @storage: #librdf_storage object
 * @context_node: #librdf_node object
 *
 * Accumulo all statements in a storage context.
 * 
 * Return value: #librdf_stream of statements or NULL on failure or context is empty
 **/
static librdf_stream*
librdf_storage_accumulo_context_serialise(librdf_storage* storage,
                                        librdf_node* context_node) 
{

    //FIXME: Not implemented.

    return 0;

}

/**
 * librdf_storage_accumulo_context_get_contexts:
 * @storage: #librdf_storage object
 *
 * Accumulo all context nodes in a storage.
 * 
 * Return value: #librdf_iterator of context_nodes or NULL on failure or no contexts
 **/
static librdf_iterator*
librdf_storage_accumulo_get_contexts(librdf_storage* storage) 
{
    // FIXME: Not implemented.

    return 0;

}

/**
 * librdf_storage_accumulo_get_feature:
 * @storage: #librdf_storage object
 * @feature: #librdf_uri feature property
 *
 * Get the value of a storage feature.
 * 
 * Return value: #librdf_node feature value or NULL if no such feature
 * exists or the value is empty.
 **/
static librdf_node*
librdf_storage_accumulo_get_feature(librdf_storage* storage, librdf_uri* feature)
{
    /* librdf_storage_accumulo_instance* scontext; */
    unsigned char *uri_string;

    /* scontext = (librdf_storage_accumulo_instance*)storage->instance; */

    if(!feature)
	return NULL;

    uri_string = librdf_uri_as_string(feature);
    if(!uri_string)
	return NULL;

    // FIXME: This is a lie.  Contexts not implemented. :-/
    if(!strcmp((const char*)uri_string, LIBRDF_MODEL_FEATURE_CONTEXTS)) {
	return librdf_new_node_from_typed_literal(storage->world,
						  (const unsigned char*)"1",
						  NULL, NULL);
    }

    return NULL;
}


/**
 * librdf_storage_accumulo_transaction_start:
 * @storage: #librdf_storage object
 *
 * Start a new transaction unless one is already active.
 * 
 * Return value: 0 if transaction successfully started, non-0 on error
 * (including a transaction already active)
 **/
static int
librdf_storage_accumulo_transaction_start(librdf_storage *storage)
{
#ifdef BROKEN

    librdf_storage_accumulo_instance* context;

    context = (librdf_storage_accumulo_instance*)storage->instance;

    /* If already have a trasaction, silently do nothing. */
    if (context->transaction)
	return 0;

    context->transaction = accumulo_elements_create();
    if (context->transaction == 0)
	return -1;

    return 0;
#endif

}


/**
 * librdf_storage_accumulo_transaction_commit:
 * @storage: #librdf_storage object
 *
 * Commit an active transaction.
 * 
 * Return value: 0 if transaction successfully committed, non-0 on error
 * (including no transaction active)
 **/
static int
librdf_storage_accumulo_transaction_commit(librdf_storage *storage)
{

#ifdef BROKEN
    librdf_storage_accumulo_instance* context;

    context = (librdf_storage_accumulo_instance*)storage->instance;

    if (context->transaction == 0)
	return -1;

    int ret = accumulo_add_elements(context->comms, context->transaction);

    accumulo_elements_free(context->transaction);

    context->transaction = 0;

    if (ret < 0) return -1;

    return 0;

#endif
}


/**
 * librdf_storage_accumulo_transaction_rollback:
 * @storage: #librdf_storage object
 *
 * Roll back an active transaction.
 * 
 * Return value: 0 if transaction successfully committed, non-0 on error
 * (including no transaction active)
 **/
static int
librdf_storage_accumulo_transaction_rollback(librdf_storage *storage)
{
#ifdef BROKEN

    librdf_storage_accumulo_instance* context;

    context = (librdf_storage_accumulo_instance*)storage->instance;

    if (context->transaction)
	return -1;

    accumulo_elements_free(context->transaction);

    context->transaction = 0;

    return 0;
#endif

}

/** Local entry point for dynamically loaded storage module */
static void
librdf_storage_accumulo_register_factory(librdf_storage_factory *factory) 
{
    LIBRDF_ASSERT_CONDITION(!strcmp(factory->name, "accumulo"));

    factory->version            = LIBRDF_STORAGE_INTERFACE_VERSION;
    factory->init               = librdf_storage_accumulo_init;
    factory->terminate          = librdf_storage_accumulo_terminate;
    factory->open               = librdf_storage_accumulo_open;
    factory->close              = librdf_storage_accumulo_close;
    factory->size               = librdf_storage_accumulo_size;
    factory->add_statement      = librdf_storage_accumulo_add_statement;
    factory->add_statements     = librdf_storage_accumulo_add_statements;
    factory->remove_statement   = librdf_storage_accumulo_remove_statement;
    factory->contains_statement = librdf_storage_accumulo_contains_statement;
    factory->serialise          = librdf_storage_accumulo_serialise;
    factory->find_statements    = librdf_storage_accumulo_find_statements;
    factory->context_add_statement    = librdf_storage_accumulo_context_add_statement;
    factory->context_remove_statement = librdf_storage_accumulo_context_remove_statement;
    factory->context_remove_statements = librdf_storage_accumulo_context_remove_statements;
    factory->context_serialise        = librdf_storage_accumulo_context_serialise;
    factory->get_contexts             = librdf_storage_accumulo_get_contexts;
    factory->get_feature              = librdf_storage_accumulo_get_feature;
    factory->transaction_start        = librdf_storage_accumulo_transaction_start;
    factory->transaction_commit       = librdf_storage_accumulo_transaction_commit;
    factory->transaction_rollback     = librdf_storage_accumulo_transaction_rollback;
}

#ifdef MODULAR_LIBRDF

/** Entry point for dynamically loaded storage module */
void
librdf_storage_module_register_factory(librdf_world *world)
{
    librdf_storage_register_factory(world, "accumulo", "Accumulo",
				    &librdf_storage_accumulo_register_factory);
}

#else

/*
 * librdf_init_storage_accumulo:
 * @world: world object
 *
 * INTERNAL - Initialise the built-in storage_accumulo module.
 */
void
librdf_init_storage_accumulo(librdf_world *world)
{
    librdf_storage_register_factory(world, "accumulo", "Accumulo",
				    &librdf_storage_accumulo_register_factory);
}

#endif

