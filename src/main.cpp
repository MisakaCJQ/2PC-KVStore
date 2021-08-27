#include <vector>
#include <getopt.h>
#include "Coordinator.h"
#include "Participant.h"
#include "MessageProcessor.h"
using namespace std;

const option long_options[]={
        {"config_path",required_argument,nullptr,'c'},
        {nullptr,0,nullptr,0}
};

int main(int argc, char* argv[]) {
    string path;

    int opt;
    while((opt= getopt_long(argc,argv,"c:",long_options, nullptr))!=-1)
    {
        if(opt=='c')
            path=optarg;
        else
            exit(EXIT_FAILURE);
    }

    /*----解析配置文件-----*/
    vector<vector<string>> configFileData=MessageProcessor::parseConfigFile(path);

    int mode=-1;
    string coordinatorIp;
    int coordinatorPort;
    vector<addr> partiAddr;

    for(auto &split_str:configFileData)
    {
        if(split_str[0]=="mode")
        {
            if(split_str[1]=="coordinator")
                mode=0;
            else if(split_str[1]=="participant")
                mode=1;
        }
        else if(split_str[0]=="coordinator_info")
        {
            coordinatorIp=split_str[1];
            coordinatorPort= stoi(split_str[2]);
        }
        else if(split_str[0]=="participant_info")
            partiAddr.push_back({split_str[1], stoi(split_str[2]),true});
    }

    /*-----启动-----*/
    if(mode==0)
    {
        Coordinator coor1(coordinatorIp,coordinatorPort,partiAddr);
        coor1.startup();
    }
    else if(mode==1)
    {
        Participant parti1(partiAddr[0].ip,partiAddr[0].port);
        parti1.startup();
    }

    //测试部分
    /*vector<addr> partiAddr{{"127.0.0.1",9832,true},{"127.0.0.1",9833,true},{"127.0.0.1",9834,true}};
    Coordinator coor1("127.0.0.1",9831,partiAddr);
    coor1.startup();*/

    /*Participant parti1("127.0.0.1",9832);
    parti1.startup();*/

    /*Participant parti2("127.0.0.1",9833);
    parti2.startup();*/

    /*Participant parti2("127.0.0.1",9834);
    parti2.startup();*/

    //vector<vector<string>> data=MessageProcessor::parseConfigFile("./config.txt");

    return 0;
}
