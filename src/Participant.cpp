#include "Participant.h"

using namespace std;

static const bool DEBUG_MODE= false;//用于打开DEBUG模式，进程会在进行事务时输出一些信息

void Participant::startup() {
    //设置协调者socket地址
    sockaddr_in coordinatoraddr=MessageProcessor::getSockAddr(ip,port);

    //获取监听socket
    int listenfd= socket(AF_INET,SOCK_STREAM,0);

    //设定监听地址重用,避免由于TCP的TIME-WAIT导致绑定地址失败
    int optval=1;
    setsockopt(listenfd,SOL_SOCKET,SO_REUSEADDR,&optval,sizeof(optval));

    if(bind(listenfd,(sockaddr*)&coordinatoraddr,sizeof(coordinatoraddr))<0)
    {//将socket绑定到地址上
        cout<<"wrong bind!"<<endl;
        cout<<ip<<':'<<port<<" might be occupied"<<endl;
        return ;
    }

    listen(listenfd,10);//开始监听
    cout<<"Participant in "+ip+':'+ to_string(port)+" start listening...\n";

    while(true)
    {
        sockaddr_in cliaddr{};
        socklen_t clilen=sizeof(cliaddr);
        //开始等待来自协调者的连接
        int connfd= accept(listenfd,(sockaddr*)&cliaddr,&clilen);
        if(handleConnection(connfd,cliaddr)<0)
            break;//发生了严重的不一致性错误
    }

}

int Participant::handleConnection(int connfd, sockaddr_in cliaddr) {
    //返回值1代表可以终止连接，返回-1代表发生不可恢复的错误，必须关机
    while(true)
    {
        string data;
        int flag= getMessage(connfd,data);//获取客户端发来的请求
        if(flag==0 || flag==-1)//如果客户端断开连接或发生读取错误，则直接返回
            return 1;

        vector<string> split_message;
        if(!MessageProcessor::parseClusterMsg(data,split_message))//解析消息不合法
            return 1;

        //for debug
        if(DEBUG_MODE)
            cout<<"connection:"<<split_message[0]<<endl;

        if(phase==1)
        {
            if(split_message[0]=="PREPARED-REQUEST")
            {
                lastResponseMsg= handlePreparePhase(split_message);
                sendMessage(connfd,lastResponseMsg);
            }
            else//异常情况
            {
                int msg_id= stoi(split_message[1]);

                if(id==msg_id)
                {//在阶段1收到错误的COMMIT或ABORT消息，且id与上一轮的id相同，则重传上一论请求的处理结果
                    sendMessage(connfd,lastResponseMsg);
                    return 1;
                }
                else//在阶段1收到错误的COMMIT或ABORT消息，且id与上一轮的id不同，说明节点数据不一致，关机
                    return -1;
            }
        }
        else if(phase==2)
        {
            if(split_message[0]=="COMMIT" || split_message[0]=="ABORT")
            {
                lastResponseMsg= handleCommitOrAbortPhase(split_message);
                sendMessage(connfd,lastResponseMsg);
                return 1;
            }
            else//异常情况
            {
                int msg_id= stoi(split_message[1]);

                if(msg_id==id)//在阶段2收到错误的PREPARED-REQUEST，且id与上一轮的id相同，则重传处理结果
                    sendMessage(connfd,lastResponseMsg);
                else
                {//在阶段2收到错误的PREPARED-REQUEST，且id与上一轮的id不同，则先进行ABORT，然后处理新的请求,然后传输结果
                    rollBack();
                    phase=1;
                    lastResponseMsg=handlePreparePhase(split_message);
                    sendMessage(connfd,lastResponseMsg);
                }
            }
        }
    }
    return -1;
}

string Participant::handlePreparePhase(const vector<string> &split_message) {
    //初始化id,事务日志,回滚日志
    id= stoi(split_message[1]);
    operationLog.clear();
    rollbackLog.clear();

    if(split_message[2]=="SET")
    {
        /*-----提取key和value，并记录到日志中-----*/
        string key=split_message[3];
        string value=split_message[4];
        operationLog.push_back({to_string(id),"SET",key,value});

        /*-----记录到回滚日志中-----*/
        if(db.count(split_message[3])==0)//key不存在，属于新增数据的情况
            rollbackLog.push_back({to_string(id),"DEL",key});
        else//key存在,属于更改数据的情况
            rollbackLog.push_back({to_string(id),"SET",key,db[key]});

        /*-----变更数据-----*/
        db[key]=value;

        //for debug
        if(DEBUG_MODE)
            cout<<id<<" SET "<<key<<' '<<value<<endl;

        phase=2;//变更阶段
        return MessageProcessor::getClusterPrepareMsg(id,"SET");
    }
    else if(split_message[2]=="GET")
    {
        string key=split_message[3];
        operationLog.push_back({to_string(id),"GET",key});

        //for debug
        if(DEBUG_MODE)
        {
            if(db.count(key)!=0)
                cout<<id<<" GET "<<key<<' '<<db[key]<<endl;
            else
                cout<<id<<" GET "<<key<<' '<<"nil"<<endl;
        }


        if(db.count(key)!=0)
            return MessageProcessor::getClusterDoneMsg(id,"GET",db[key]);
        else
            return MessageProcessor::getClusterDoneMsg(id,"GET","nil");
        //阶段不变更
    }
    else if(split_message[2]=="DEL")
    {
        auto len=split_message.size();
        for(auto i=3;i<len;i++)
        {
            string key=split_message[i];
            if(db.count(key)!=0)
            {
                operationLog.push_back({to_string(id),"DEL",key,"ACK"});
                rollbackLog.push_back({to_string(id),"SET",key,db[key]});
                db.erase(key);

                //for debug
                if(DEBUG_MODE)
                    cout<<id<<" DEL "<<key<<" ACK "<<endl;
            }
            else
            {
                operationLog.push_back({to_string(id),"DEL",key,"WRONG"});

                //for debug
                if(DEBUG_MODE)
                    cout<<id<<" DEL "<<key<<" WRONG "<<endl;
            }
        }

        phase=2;//变更阶段
        return MessageProcessor::getClusterPrepareMsg(id,"DEL");
    }
    return "";
}

string Participant::handleCommitOrAbortPhase(const vector<string> &split_message) {
    phase=1;//变更阶段
    if(split_message[0]=="COMMIT")
    {
        //for debug
        if(DEBUG_MODE)
            cout<<"COMMIT"<<endl;

        if(operationLog[0][1]=="DEL")
        {//DEL方法需要统计删除成功的个数
            int ACKcount=0;
            for(const auto &operation:operationLog)
                if(operation[3]=="ACK")
                    ACKcount++;
                return MessageProcessor::getClusterDoneMsg(id,"DEL", to_string(ACKcount));
        }
        else
            return MessageProcessor::getClusterDoneMsg(id,operationLog[0][1],"");
    }
    else if(split_message[0]=="ABORT")
    {
        //for debug
        if(DEBUG_MODE)
            cout<<"ABORT"<<endl;
        rollBack();
        return MessageProcessor::getClusterDoneMsg(id,operationLog[0][1],"");
    }


    return "";
}

int Participant::handleExceptionMessage(const vector<string> &split_message,string &responseMsg) {
    int msg_id= stoi(split_message[1]);
    if(msg_id==id)
    {
        //在阶段1收到错误的COMMIT或ABORT消息，且id与上一轮的id相同，则重传处理结果
        //在阶段2收到错误的PREPARED-REQUEST，且id与上一轮的id相同，则重传处理结果
        responseMsg=lastResponseMsg;
        return 0;
    }
    else//id不一致的情况
    {
        if(phase==1)//在阶段1收到错误的COMMIT或ABORT消息，且id与上一轮的id不同，说明节点数据不一致，关机
            return 1;
        else if(phase==2)
        {//在阶段2收到错误的PREPARED-REQUEST，且id与上一轮的id不同，则先进行ABORT，然后处理新的请求
            rollBack();
            phase=1;
            responseMsg= handlePreparePhase(split_message);
            return 1;
        }
    }

    return 0;
}

int Participant::getMessage(int connfd, string &data) const {
    vector<char> buf(MAX_BUF_SIZE);//定义缓冲区
    ssize_t byterecv=0;
    do
    {
        byterecv= recv(connfd,&buf[0],MAX_BUF_SIZE,0);
        if(byterecv<0)
        {//发生其他错误
            cout<<"wrong recv!\n";
            return -1;
        }
        else if (byterecv==0)//客户端关闭连接
            return 0;
        else
            data.append(buf.cbegin(),buf.cbegin()+byterecv);
    }
    while(byterecv==MAX_BUF_SIZE);

    return 1;
}

int Participant::sendMessage(int connfd, const string &data) {
    if(data.empty())
        return 0;

    if(send(connfd,data.c_str(),data.size(),0)<0)
        return -1;

    return 1;
}

void Participant::rollBack() {
    if(rollbackLog.empty())
        return;

    //for debug
    if(DEBUG_MODE)
        cout<<"roll back";

    for(const auto &operation:rollbackLog)
    {
        if(operation[1]=="SET")
        {
            string key=operation[2];
            string value=operation[3];
            db[key]=value;
        }
        else if(operation[1]=="DEL")
        {
            string key=operation[2];
            if(db.count(key))
                db.erase(key);
        }
    }
    //回滚完毕后将回滚日志清空
    rollbackLog.clear();
}


