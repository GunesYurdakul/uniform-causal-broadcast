#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <ctime>
#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <tuple>
#include <map>
#include <vector>
#include <thread>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <algorithm>
#include <tuple>
#include <cmath>
#include <queue>
#include<sstream>
#define MAXRECVSTRING 255

using namespace std;

static int wait_for_start = 1;
int sock; //socket
unsigned int total_process; //number of total processes
int lsn;//sequence number of the current process
vector<string> log_vector; //log vector

class Receive_packet {

public:
  int sender;
  tuple <int,int> orig_sender_sn_tuple;
  vector<int> vector_clock;
  Receive_packet();
  ~ Receive_packet(){};
  Receive_packet(int *);
  Receive_packet (int sender, string packet_data);
  Receive_packet(int, int, int, vector<int> v);
};


Receive_packet:: Receive_packet (int sender, int orig_sender,int sn, vector<int> vector_clock){
  this->sender = sender;
  this->orig_sender_sn_tuple = make_tuple(orig_sender,sn);
  this->vector_clock = vector_clock;
}

Receive_packet:: Receive_packet (int sender, string packet_string){

  istringstream iss(packet_string);
  int * packet_data;
  packet_data=new int[total_process+2];
  int data;
  int i=0;

  //splitting packet string by space character and convert to integer
  while(iss>>data){
    packet_data[i]=data;
    i++;
  }

  this->sender = sender;
  this->orig_sender_sn_tuple = make_tuple(packet_data[0], packet_data[1]);
  for (unsigned int i=2; i<total_process+2;i++){
        this->vector_clock.push_back(packet_data[i]);
  }
  //log_vector.push_back("receive " + to_string(packet_data[0])+" "+to_string(packet_data[1]));
}
// global variables


int causal_lsn;//sequence number of the current process causal broadcasts
vector< Receive_packet*> causal_pending; // list of pending messages for causal broadcast
vector <int> vector_clock; // keeps the vector clock of current process
map < int ,vector<int>> dependancy_processes; //maps a process i to other processes which can affect process i

vector< Receive_packet*> pending; // list of pending messages for FIFO
map < tuple<int,int>,vector<int>> ack; //list of processes wich sent acknowledge for specific messages
queue< Receive_packet*> ack_messages; // the list checked by "acknowledge broadcasting thread", if there is a message to broadcast
vector< tuple<int,int>> forward_urb; // list of pending messages for URB
vector <int> next_list; // keeps the next sequence number for each process to deliver
vector<tuple<int,int>> delivered;// delivered message for URB
map <unsigned int, int> port_to_id;//maps each port to its corresponding id
vector <struct sockaddr_in> broadcastAddresses; // list of ports of other processes
int number_of_messages;//number of messages to broadcast
unsigned short port_num; // port number

//best effort broadcasting
void beb_broadcast(int packet[]) {

    vector <struct sockaddr_in> adresses;
    string message;

    // send message as a character array: "sender lsn vector_clock[0] vector_clock[1] ... vector_clock[n] ",
    for (unsigned int i = 0; i<total_process+2; i++)
      message += (to_string(packet[i]) + " ");

    log_vector.push_back("b "+to_string(packet[1]));

    const char *array = message.c_str();
    //sends messages to all processes in broadcastAddresses list.
    for (vector<struct sockaddr_in>::iterator send_adress = broadcastAddresses.begin(); send_adress != broadcastAddresses.end(); ++send_adress ){
        //sending message via UDP packet
        if (sendto(sock, array, strlen(array), 0, (struct sockaddr *) &(*send_adress), sizeof(*send_adress)) != sizeof(array)){
        }
        usleep(20000);   /* Avoids flooding the network */
    }
}

// called if SIGTERM or SIGINT signals are received
static void stop(int sig) {

    ofstream f_output;
    f_output.open("da_proc_"+to_string(port_to_id[port_num])+".out");
    signal(SIGTERM, SIG_DFL);
    signal(SIGINT, SIG_DFL);

    // all log messages are written to log file just before process is terminated.
    for (unsigned int i=0;i<log_vector.size();i++)
    {
        f_output<<log_vector[i]<<endl;
    }
    //immediately stop network packet processing
    cout << "Immediately stopping network packet processing." << endl;

    cout << "Writing output." << endl;

    f_output.close();
    exit(0);
    //exit directly from signal handler

}

//start broadcasting
static void start(int signum) {
    wait_for_start = 0;
    lsn=0;
    causal_lsn=0;

    vector<char *> messages;
    //initialize next_list and vector_clock list with 0s
    for (unsigned int i = 0; i<total_process; i++)
    {
      next_list.push_back(1);
      vector_clock.push_back(0);
    }
}


// Broadcasts ACK messages
void beb_ack_broadcast(){
    int *array;
    array = new int[total_process+2];
    vector <struct sockaddr_in> adresses;
    Receive_packet *ack_message;


    while(true){
        // if a message is receiced acknowledge this send ACK message to all processes
        if(ack_messages.size()>0){// check if there is a ack message to broadcast
            ack_message = ack_messages.front();
            array[0]=get <0> (ack_message->orig_sender_sn_tuple);
            array[1]=get <1> (ack_message->orig_sender_sn_tuple);

            for(unsigned int i =0 ; i<total_process; i++)
            {
              array[i+2]=(ack_message->vector_clock)[i];
            }

            string message;
            for (unsigned int i = 0; i<total_process+2; i++)
              message += (to_string(array[i]) + " ");
            const char *array_send = message.c_str();

            for (vector<struct sockaddr_in>::iterator send_adress = broadcastAddresses.begin(); send_adress != broadcastAddresses.end(); ++send_adress ){

                if (sendto(sock, array_send, strlen(array_send), 0, (struct sockaddr *) &(*send_adress), sizeof(*send_adress)) != sizeof(array_send))
                    sleep(0);   /* Avoids flooding the network */
                usleep(100);   /* Avoids flooding the network */

            }
            ack_messages.pop();
        }
    }
}


//uniform reliable broadcasting
void urb_broadcast(vector <int> w) {
    tuple <int,int> message_tuple = make_tuple(port_to_id[port_num],lsn);
    int *array;
    array = new int[total_process+2];
    array[0]=port_to_id[port_num];
    array[1]=lsn;

    for(unsigned int i =0 ; i<total_process; i++)
    {
      array[i+2]=w[i];
    }
    // add to forward list
    forward_urb.push_back(message_tuple);
    //trigger beb broadcast
    beb_broadcast(array);
}

//fifo reliable brpadcasting
void frb_broadcast(vector <int> w){
    //increment lsb
    lsn++;
    //trigger urb broadcasting
    urb_broadcast(w);
}

// causal broadcasr
void crb_broadcast(){
  vector<int> w = vector_clock;
  //vector clock of message to send
  w[port_to_id[port_num]-1]=causal_lsn;
  causal_lsn ++;
  //trigger fifo broadcast
  frb_broadcast(w);
}


// check only processes which affect the sender of v2
bool compare_clock_vectors(int orig_sender,vector<int> v1,vector<int> v2) {

  //iterate over processes which affect procces orig_sender
  for (unsigned int i=0; i<dependancy_processes[orig_sender].size(); i++){
    int dependant_process = dependancy_processes[orig_sender][i] - 1;
    if (v2[dependant_process]>v1[dependant_process])
      return false;
  }
  return true;
}

void frb_deliver(Receive_packet *incoming_packet){
  causal_pending.push_back(incoming_packet);
  for (vector<Receive_packet *>::iterator packet = causal_pending.begin(); packet != causal_pending.end(); ){

      // check if the current process' vector clock is greater or equal to received packet in pending list
      if(compare_clock_vectors( get <0> ((*packet)->orig_sender_sn_tuple), vector_clock, (*packet)->vector_clock)){

          // causal delivering of the message
          vector_clock[(get <0> ((*packet)->orig_sender_sn_tuple)) - 1]++;
          //add log delivery message
          log_vector.push_back("d "+to_string(get <0> ((*packet)->orig_sender_sn_tuple))+ " " +to_string(get <1> ((*packet)->orig_sender_sn_tuple)));
          //erase packet from causal pending list
          causal_pending.erase(packet);
          if(causal_pending.size()>0){
            packet=causal_pending.begin();
          }
      }
      else{
          ++packet;
      }
  }

}


// trigger frb_broadcast for each message
void frb_all_broadcast() {
    for(int i=0; i<number_of_messages; i++){
        crb_broadcast();
    }
}

//uniform reliable deliver
void urb_deliver(Receive_packet *incoming_packet){
    pending.push_back(incoming_packet);
    int orig_sender;
    int sn;
    for (vector<Receive_packet*>::iterator message_it = pending.begin(); message_it != pending.end(); ){
        orig_sender= get <0> ((*message_it)->orig_sender_sn_tuple);
        sn= get <1> ((*message_it)->orig_sender_sn_tuple);

        // check if the sn of the received message is equal to expected sn of that process
        if(next_list[orig_sender-1] == sn){
            next_list[orig_sender-1]++;
            //log_vector.push_back("fifo"+to_string(orig_sender)+" "+to_string(sn));
            frb_deliver(*message_it);
            pending.erase(message_it);
        }
        // if no elemenr of pending vector is deleted
        else
            ++message_it;
    }
}

// check if a element is contained in the given vector
bool in_vector(vector<tuple<int,int>> v,tuple<int,int> search_tuple){
    return find(v.begin(), v.end(), search_tuple) != v.end();
}

//best effort deliver
void beb_deliver(Receive_packet *incoming_packet){
    tuple <int,int> message_tuple = incoming_packet->orig_sender_sn_tuple;
    ack[message_tuple].push_back(incoming_packet->sender);

    //check if message is in the forward list
    if (find(forward_urb.begin(), forward_urb.end(), message_tuple) == forward_urb.end())
    {
        forward_urb.push_back(message_tuple);
        ack_messages.push(incoming_packet);
    }
    // check if the message is forwarded and received by enough number of processes and is not present in the already delivered messages, then deliver it (URB)
    if (in_vector(forward_urb,message_tuple) && !in_vector(delivered,message_tuple) && ack[message_tuple].size() > floor(total_process/2))
    {
        //log_vector.push_back("beb "+to_string(get <0> ((incoming_packet)->orig_sender_sn_tuple))+" "+to_string(get <1> ((incoming_packet)->orig_sender_sn_tuple)));
        delivered.push_back(message_tuple);
        urb_deliver(incoming_packet);
    }
}

// receive p2p packet via UDP
void receive_udp() {

    sockaddr_in client;
    int len = sizeof(client);
    char *array;
    array = new char[1000];
    int n=0;

    while (true) {

        if ((n = recvfrom(sock, (char *)array, 10000, 0, (struct sockaddr *)&client, (socklen_t *)&len)) < 0){
            cout << "recvfrom() failed" << endl;
            return;
        }
        array[n]='\0';
        string message(array);

        // convert to Receive_packet object
        unsigned int sender = port_to_id[ntohs(client.sin_port)];
        Receive_packet *incoming_packet = new Receive_packet(sender, message);
        beb_deliver(incoming_packet);
    }
}



int main(int argc, char** argv) {

    //set signal handlers
    signal(SIGUSR2, start); // 10
    signal(SIGTERM, stop); // 15
    signal(SIGINT, stop); // 2

    //variables
    vector<int> msg_array;
    vector<int> correct_procs;

    // UDP connection members
    struct sockaddr_in my_broadcastAddr; /* Broadcast address */


    //parse arguments, including membership
    //initialize application
    //start listening for incoming UDP packets
    if(argc<4){
        printf("Number of given paramaters %d\n",argc );
        cout<< "Missing Parameter" <<endl;
        return 0;
    }

    unsigned int process_id = atoi(argv[1]);
    number_of_messages = atoi(argv[3]);
    string input_filename = argv[2];

    //open membership file
    ifstream mem_file;
    mem_file.open(input_filename);

    string line;
    string process_specs;

    getline(mem_file, process_specs);

    //number of tatal processes
    total_process= atoi(process_specs.c_str());

    // read membership file add each process to our broadcast list
    for (unsigned int n_line = 1; getline (mem_file,line) ; ++n_line){

      const char *idx_c = strtok(const_cast<char*>(line.c_str()), " ");
      if(idx_c==NULL)
        break;
        
      int idx = atoi(idx_c);
      if (n_line<=total_process)
      {
        struct sockaddr_in broadcastAddr;
        if(strcmp(line.c_str(),"")==0)
            break;
        char *proc_ip;
        int proc_port;
        proc_ip = strtok(NULL, " ");
        proc_port = atoi(strtok(NULL, " "));
        port_to_id[proc_port] = idx;

        //if current process
        if (n_line == process_id) {

            memset(&my_broadcastAddr, 0, sizeof(my_broadcastAddr));   /* Zero out structure */
            my_broadcastAddr.sin_family = AF_INET;                 /* Internet address family */
            my_broadcastAddr.sin_addr.s_addr = htonl(INADDR_ANY);  /* Any incoming interface */
            my_broadcastAddr.sin_port = htons(proc_port);      /* Broadcast port */
            port_num=proc_port;
        }
        memset(&broadcastAddr, 0, sizeof(broadcastAddr));   /* Zero out structure */
        broadcastAddr.sin_family = AF_INET;                 /* Internet address family */
        broadcastAddr.sin_addr.s_addr = inet_addr(proc_ip);/* Broadcast IP address */
        broadcastAddr.sin_port = htons(proc_port);         /* Broadcast port */
        broadcastAddresses.push_back(broadcastAddr);
      }
      else{
        //tokenize line

        char *dependant_process = strtok(NULL, " ");

        //add processes which affect process 'idx'(key) as a list(value)
        if (dependant_process!=NULL){
          dependancy_processes[idx].push_back(atoi(dependant_process));
        }

        while (dependant_process) {

          dependant_process = strtok(NULL, " ");

          if (dependant_process == NULL) break;

          dependancy_processes[idx].push_back(atoi(dependant_process));//add as process which affects process_idx
        }

      }

    }
    if ((sock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0){
        cout << "socket() failed" << endl;
    }
    else
        cout<<"Socket created"<<endl;

    if (bind(sock, (struct sockaddr *) &my_broadcastAddr, sizeof(my_broadcastAddr)) < 0){
        cout << "bind() failed" << endl;
    }

    //wait until start signal
    while(wait_for_start) {
        struct timespec sleep_time;
        sleep_time.tv_sec = 0;
        sleep_time.tv_nsec = 1000;
        nanosleep(&sleep_time, NULL);
    }

    //broadcast messages
    cout << "Broadcasting messages." << endl;
    //start threads
    thread ack_send(beb_ack_broadcast);
    thread receive(receive_udp);
    thread send(frb_all_broadcast);
    //join all threads
    send.join();
    receive.join();
    ack_send.join();
    //wait until stopped
    while(1) {
        struct timespec sleep_time;
        sleep_time.tv_sec = 1;
        sleep_time.tv_nsec = 0;
        nanosleep(&sleep_time, NULL);
    }
    //close the socket
    close(sock);
}
