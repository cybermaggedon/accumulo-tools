
CFLAGS=-I/usr/include/raptor2 -I/usr/include/rasqal -fPIC -I. -Iredland -g
CXXFLAGS=-I/usr/include/raptor2 -I/usr/include/rasqal -fPIC -I. -Iredland -g
LIBS=-lrasqal -lrdf

SQLITE_FLAGS=-DSTORE=\"sqlite\" -DSTORE_NAME=\"STORE.db\"

ACCUMULO_FLAGS=-DSTORE=\"accumulo\" -DSTORE_NAME=\"gaffer:42424\"

LIB_OBJS=AccumuloAPI.o TableOperations.o AccumuloProxy.o proxy_types.o \
	proxy_constants.o accumulo_comms.o

all: test-sqlite test-accumulo librdf_storage_accumulo.so

libaccumulo.a: ${LIB_OBJS}
	${AR} cr $@ ${LIB_OBJS}
	ranlib $@

test-sqlite: test-sqlite.o
	${CXX} ${CXXFLAGS} test-sqlite.o -o $@ ${LIBS}

test-accumulo: test-accumulo.o
	${CXX} ${CXXFLAGS} test-accumulo.o -o $@ ${LIBS}

bulk_load: bulk_load.o
	${CXX} ${CXXFLAGS} bulk_load.o -o $@ ${LIBS}

test-sqlite.o: test.C
	${CXX} ${CXXFLAGS} -c $< -o $@  ${SQLITE_FLAGS}

test-accumulo.o: test.C
	${CXX} ${CXXFLAGS} -c $< -o $@ ${ACCUMULO_FLAGS}

ACCUMULO_OBJECTS=accumulo.o libaccumulo.a

librdf_storage_accumulo.so: ${ACCUMULO_OBJECTS}
	${CXX} ${CXXFLAGS} -shared -o $@ ${ACCUMULO_OBJECTS} -lthrift

accumulo.o: CFLAGS += -DHAVE_CONFIG_H -DLIBRDF_INTERNAL=1 

install: all
	sudo cp librdf_storage_accumulo.so /usr/lib64/redland

depend:
	makedepend -Y -I. *.c *.C

# DO NOT DELETE

gaffer.o: ./gaffer_comms.h ./gaffer_query.h
gaffer_comms.o: ./gaffer_comms.h ./gaffer_query.h
gaffer_query.o: ./gaffer_query.h
