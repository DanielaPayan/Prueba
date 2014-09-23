#include <iostream>
#include <set>
#include <string>
#include <queue>
#include <unordered_map>
#include <czmq.h>
#include <string.h>
#include <cstdlib>

using namespace std;

typedef unordered_map<string, queue<zframe_t*>> WorkerReferences;
WorkerReferences wr;
queue<zframe_t*> wl;  //Cola para ids de workers libres
queue<zmsg_t*> men;   //Cola para mensajes en espera


void registerWorker(zframe_t* id, string operation) {
  zframe_print(id, "Id to add");
  zframe_t* dup = zframe_dup(id); //COpia
  zframe_print(dup, "Id copy add");
  wr[operation].push(dup);
  // wl.push(dup);  //Guardo el worker en la cola de worker libres

}

zframe_t* getWorkerFor(string operation) {

 
  zframe_t* wid = wl.front();
  wl.pop(); 
  zframe_print(wid,"Worker seleccionado");

  return zframe_dup(wid);
}

void handleClientMessage(zmsg_t* msg, void* workers) {
  cout << "Handling the following message" << endl;
  zmsg_print(msg);

  zframe_t* clientId = zmsg_pop(msg);

  char* operation = zmsg_popstr(msg);
  zframe_t* worker = getWorkerFor(operation);

  // char* reprWId = zframe_strhex(worker);
  // cout << "Selected worker to handle request: " << reprWId << endl;
  // free(reprWId);
  zmsg_pushstr(msg, operation);
  zmsg_prepend(msg, &clientId);
  zmsg_prepend(msg, &worker);

  // Prepare and send the message to the worker
  zmsg_send(&msg, workers);

  cout << "End of handling" << endl;
  // zframe_destroy(&clientId);
  free(operation);
  zmsg_destroy(&msg);
}



void handleWorkerMessage(zmsg_t* msg, void* clients) {
  cout << "Handling the following WORKER" << endl;
  zmsg_print(msg);
  // Retrieve the identity and the operation code
  zframe_t* id = zmsg_pop(msg);
  wl.push(id);
  char* opcode = zmsg_popstr(msg);
  if (strcmp(opcode, "register") == 0) {
    // Get the operation the worker computes
    char* operation = zmsg_popstr(msg);
    // Register the worker in the server state
    registerWorker(id, operation);
    free(operation);
  } else if (strcmp(opcode, "answer") == 0) {

    zmsg_send(&msg, clients);


  } else {
    cout << "Unhandled message" << endl;
  }
  cout << "End of handling" << endl;
  free(opcode);
  zframe_destroy(&id);
  zmsg_destroy(&msg);
}

int main(void) {
  zctx_t* context = zctx_new();
  // Socket to talk to the workers
  void* workers = zsocket_new(context, ZMQ_ROUTER);
  int workerPort = zsocket_bind(workers, "tcp://*:5555");
  cout << "Listen to workers at: "
       << "localhost:" << workerPort << endl;

  // Socket to talk to the clients
  void* clients = zsocket_new(context, ZMQ_ROUTER);
  int clientPort = zsocket_bind(clients, "tcp://*:4444");
  cout << "Listen to clients at: "
       << "localhost:" << clientPort << endl;

  zmq_pollitem_t items[] = {{workers, 0, ZMQ_POLLIN, 0},
                            {clients, 0, ZMQ_POLLIN, 0}};
  cout << "Listening!" << endl;

  while (true) {

    zmq_poll(items, 2, 10 * ZMQ_POLL_MSEC);
    if (items[0].revents & ZMQ_POLLIN) {
      cerr << "From workers\n";
      zmsg_t* msg = zmsg_recv(workers);
      handleWorkerMessage(msg, clients);
    }
    if (items[1].revents & ZMQ_POLLIN) {
      cerr << "From clients\n";
      zmsg_t* msg = zmsg_recv(clients);
      //men.push(msg);

      if(!wl.empty()){
        handleClientMessage(msg, workers); //Mando el mensaje a un worker
      }else{
        men.push(msg);  //Meto el mensaje a la cola
      }
    }

    if(!men.empty()&& !wl.empty()){
      handleClientMessage(men.front(),workers); /// el primer mensaje por ese socket
      men.pop(); //elimino ese mensaje

    }
  }

  // TODO: Destroy the identities

  zctx_destroy(&context);
  return 0;
}
