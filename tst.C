#include "../client/AccumuloAPI.h"

void add_triple(BatchWriter writer,
		const std::string& s, const std::string& p,
		const std::string& o)
{

    const long double now = time(0)*1000;

    Mutation mut1(s + ":" + p);
    mut1.put("spo", o, "", now, o);

    Mutation mut2(p + ":" + o);
    mut2.put("pos", s, "", now, s);

    Mutation mut3(o + ":" + s);
    mut3.put("osp", p, "", now, p);
    
    writer.addMutation(mut1);
    writer.addMutation(mut2);
    writer.addMutation(mut3);

}

int main(int argc, char* argv[])
{

    Connector connector("localhost4", 42424, "root", "accumulo");

    Authorizations auths("");

    try {
	connector.tableOperations().createTable("mytest");
    } catch (TableExistsException& e) {
	std::cerr << "Create table failed ignored." << std::endl;
    }

    BatchWriter writer = connector.createBatchWriter("mytest", 5000, 10000,
						     10000, 2);

    add_triple(writer, "lion", "eats", "zebra");
    add_triple(writer, "lion", "has-a", "tail");
    add_triple(writer, "lion", "has-a", "mane");
    add_triple(writer, "hyena", "eats", "zebra");

    writer.flush();
    writer.close();

    // ------------------------------

    std::cerr << "lion,," << std::endl;

    Scanner scanner = connector.createScanner("mytest", auths);

    Key ks, ke;
    ks.row = "lion:"; ke.row="lion;";

    Range range;
    range.start = ks;
    range.stop = ke;

    scanner.setRange(range);
    scanner.fetchColumnFamily("spo");

    ScannerIterator itr = scanner.iterator();

    while(itr.hasNext()) {
	KeyValue kv = itr.next();

	cout << kv.key.row << " " << kv.key.colFamily << ":" 
	     << kv.key.colQualifier << " [" << kv.key.colVisibility
	     << "] " << kv.key.timestamp << "\t" << kv.value << "\n";

    }

    // ------------------------------

    std::cerr << ",,zebra" << std::endl;

    scanner = connector.createScanner("mytest", auths);

    ks.row = "zebra:"; ke.row="zebra;";

    range.start = ks;
    range.stop = ke;

    scanner.setRange(range);
    scanner.fetchColumnFamily("osp");

    itr = scanner.iterator();

    while(itr.hasNext()) {
	KeyValue kv = itr.next();

	cout << kv.key.row << " " << kv.key.colFamily << ":" 
	     << kv.key.colQualifier << " [" << kv.key.colVisibility
	     << "] " << kv.key.timestamp << "\t" << kv.value << "\n";

    }

    itr.close();

    // ------------------------------

    std::cerr << "All done" << std::endl;

    connector.close();



#ifdef ASDASDASD

    if(argc < 8 || argc > 10) {
	cout << "Usage: " << argv[0] << " <host> <port> <username> <password> <tableName> <startRowId> <stopRowId> <colFam> <colQual>\n";	
	return 1;
    }
	
    try {
		
	Connector connector(argv[1], atoi(argv[2]), argv[3], argv[4]);

	Authorizations auths("");
	Scanner scanner = connector.createScanner(argv[5], auths);

	Key ks, ke;
	ks.row = argv[6]; ke.row = argv[7];

	Range range;
	range.start = ks;
	range.stop = ke;

	scanner.setRange(range);

	if(argc > 8) {

	    if(argc == 9) {

		scanner.fetchColumnFamily(argv[8]);
	    }

	    else if(argc == 10) {

		scanner.fetchColumn(argv[8], argv[9]);
	    }
	}

	ScannerIterator itr = scanner.iterator();

	while(itr.hasNext()) {
	    KeyValue kv = itr.next();

	    cout << kv.key.row << " " << kv.key.colFamily << ":" 
		 << kv.key.colQualifier << " [" << kv.key.colVisibility
		 << "] " << kv.key.timestamp << "\t" << kv.value << "\n";

	}

	itr.close();
	connector.close();

    } catch(TableNotFoundException &e) {
	cout << "The specified table was not found\n";
    }
	
    return 0;

#endif
}
