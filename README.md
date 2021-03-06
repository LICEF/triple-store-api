triple-store-api
================

Triple Store application server with basic services over Jena

Usage
-----
1) create a TripleStore object
```
TripleStore tripleStore = new TripleStore(); //see other constructor for store data elsewhere
```
2) if you want open a SPARQL Endpoint, start the Jena Fuseki server like this :
```
tripleStore.startServer(true); //false for accept write access on endpoint (be careful)
```

3) interact within the Jena API

The present triple-store-api interact with Jena in transactional mode. It lets you to call a sequence of TripleStore methods from the execution stack.
To do this, a simili-closure process is provided with the function `transactionalCall` which take the initial function to call. A commit (or rollback if error) will be done at the end of the call.
The following `test` method starts the transaction:
```
void test() {
    //creation of the invocation method
    Invoker inv = new Invoker(this, "current.package", "writeTriple", new Object[]{ "Hello World" } );
    //start transaction in write mode because at least one write access
    Object resp = tripleStore.transactionalCall(inv, TripleStore.WRITE_MODE);
    System.out.println( (String)resp ); //"Transaction done!" will be displayed
} 

String writeTriple(String message) {
    Triple triple = new Triple( "http://myResource1", RDFS.label, message );
    tripleStore.insertTriple( triple );
    return displayResult();
}    

String displayResult() {
    Triple[] triples = tripleStore.getTriplesWithSubject( "http://myResource1" );
    for ( Triple t : triples)
        System.out.println( t );
    return "Transaction done!";
}    


```
Furthermore, transactions are multi-thread safe. You can have only one active write transaction but many others read transactions at the same time. All of them are independents.
