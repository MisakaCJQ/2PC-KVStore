#include "Coordinator.h"
using namespace std;

static const bool DEBUG_MODE= false;//用于打开DEBUG模式，进程会在进行事务时输出一些信息

void Coordinator::startup() {
    //设置协调者socket地址
    sockaddr_in coordinatoraddr=MessageProcessor::getSockAddr(curAddr.ip,curAddr.port);

    //获取监听socket
    int listenfd= socket(AF_INET,SOCK_STREAM,0);

    //设定监听地址重用,避免由于TCP的TIME-WAIT导致绑定地址失败
    int optval=1;
    setsockopt(listenfd,SOL_SOCKET,SO_REUSEADDR,&optval,sizeof(optval));//设定监听端口重用

    if(bind(listenfd,(sockaddr*)&coordinatoraddr,sizeof(coordinatoraddr))<0)
    {//将socket绑定到地址上
        cout<<"wrong bind!"<<endl;
        cout<<curAddr.ip<<':'<<curAddr.port<<" might be occupied"<<endl;
        return ;
    }

    listen(listenfd,10);//开始监听
    cout<<"Coordinator in "+curAddr.ip+':'+ to_string(curAddr.port)+" start listening...\n";

    while(true)
    {
        sockaddr_in cliaddr{};
        socklen_t clilen=sizeof(cliaddr);
        //开始等待连接
        int connfd= accept(listenfd,(sockaddr*)&cliaddr,&clilen);
        handleConnection(connfd,cliaddr);
        close(connfd);
    }
}

void Coordinator::handleConnection(int connfd, sockaddr_in cliaddr) {
    //for debug
    if(DEBUG_MODE)
        cout<<"Connection comming"<<endl;

    string data;
    int flag= getMessage(connfd,data);//获取客户端发来的请求
    if(flag==0 || flag==-1)//如果客户端断开连接或发生读取错误，则直接返回
        return;

    vector<string> split_message;
    if(!MessageProcessor::parseClientMsg(data,split_message))
    {//客户端消息解析错误，返回ERROR信息
        sendMessage(connfd,MessageProcessor::getClientERRORMsg());
        return;
    }

    //根据操作类型选择对应的处理函数
    if(split_message[0]=="SET")
        sendMessage(connfd, handleSET(split_message));
    else if(split_message[0]=="GET")
        sendMessage(connfd, handleGET(split_message));
    else if(split_message[0]=="DEL")
        sendMessage(connfd, handleDEL(split_message));
}

string Coordinator::handleSET(const vector<string> &split_message) {
    vector<int> participantSocket(participantCount,-1);//保存所有参与者的套接字描述符，-1代表未连接
    int aliveParticipantCount=connectAllParticipants(participantSocket);

    if(aliveParticipantCount==0)//与所有参与者节点失联，直接返回ERROR消息
        return MessageProcessor::getClientERRORMsg();

    /*---------第一阶段---------*/

    //向所有参与者发送PREPARED-REQUEST请求
    int id=getRandomID();//为本次事务申请一个随机id
    string preparedRequestMsg= MessageProcessor::getClusterPrepareRequestMsg(id, split_message);
    for(const int ps:participantSocket)
        if(ps>0)
            sendMessage(ps,preparedRequestMsg);

    //等待所有参与者发回prepared
    int prepareResponse= waitForPrepared(participantSocket,aliveParticipantCount,preparedRequestMsg);

    //prepareResponse为0代表至少有一个参与者返回了NO，为1代表全部发回prepare，为-1代表参与者全挂了

    if(prepareResponse==0 || prepareResponse==1)//参与者全都发回prepared或至少有一个参与者返回NO
    {
        /*--------------第二阶段--------------------*/
        //向所有参与者发送COMMIT或ABORT请求
        string commitOrAbortRequestMsg;
        if(prepareResponse==0)
            commitOrAbortRequestMsg=MessageProcessor::getClusterAbortMsg(id);
        else
            commitOrAbortRequestMsg=MessageProcessor::getClusterCommitMsg(id);

        for(const int ps:participantSocket)
            if(ps>0)
                sendMessage(ps, commitOrAbortRequestMsg);

        //等待参与者发回DONE
        vector<string> response_split_msg;
        int commitResponse= waitForCommitOrAbort(participantSocket, aliveParticipantCount, commitOrAbortRequestMsg, response_split_msg);

        if(commitResponse==1 && prepareResponse==1)//收到了所有存活参与者的DONE反馈,且发出的是commit指令
            return MessageProcessor::getClientOKMsg();
        else//在第二阶段中参与者全挂了，或收到了所有存活参与者的DONE反馈但是ABORT指令
            return MessageProcessor::getClientERRORMsg();//直接返回ERROR消息
    }
    else//在第一阶段中参与者全挂了
    {//直接返回ERROR消息
        return MessageProcessor::getClientERRORMsg();
    }
}

string Coordinator::handleGET(const vector<string> &split_message) {
    vector<int> participantSocket(participantCount,-1);//保存所有参与者的套接字描述符，-1代表未连接
    int aliveParticipant=1;

    int id=getRandomID();//为本次事务申请一个随机id
    string getRequestMsg= MessageProcessor::getClusterPrepareRequestMsg(id, split_message);
    //向其中一个存活的参与者询问即可
    for(int i=0;i<participantCount;i++)
    {
        if(participantAddr[i].vaild)
        {
            int sockfd=connectParticipant(i);
            if(sockfd>0)//连接成功
            {
                aliveParticipant=1;
                participantSocket[i]=sockfd;
                sendMessage(sockfd,getRequestMsg);
                vector<string> response_split_msg;
                //这里直接采用第二阶段的等待函数，GET操作不用第一阶段回prepared,只需要发PREPARED-REQUEST，参与者直接回DONE和查询结果就行
                if(waitForCommitOrAbort(participantSocket,aliveParticipant,getRequestMsg,response_split_msg)>0)
                    return MessageProcessor::getClientGetOrDelMsg(id,response_split_msg);

                //如果等待查询返回的信息失败，则participantSocket数组会被自动重置，socket也会被关闭
            }
        }
    }

    //进行到这里说明所有参与者都查询失败，全都挂了
    return MessageProcessor::getClientERRORMsg();
}

string Coordinator::handleDEL(const vector<string> &split_message) {
    vector<int> participantSocket(participantCount,-1);//保存所有参与者的套接字描述符，-1代表未连接
    int aliveParticipantCount=connectAllParticipants(participantSocket);

    if(aliveParticipantCount==0)//与所有参与者节点失联，直接返回ERROR消息
        return MessageProcessor::getClientERRORMsg();

    /*---------第一阶段---------*/

    //向所有参与者发送PREPARED-REQUEST请求
    int id=getRandomID();//为本次事务申请一个随机id
    string preparedRequestMsg= MessageProcessor::getClusterPrepareRequestMsg(id, split_message);
    for(const int ps:participantSocket)
        if(ps>0)
            sendMessage(ps,preparedRequestMsg);

    //等待所有参与者发回prepared
    int prepareResponse= waitForPrepared(participantSocket,aliveParticipantCount,preparedRequestMsg);

    //prepareResponse为0代表至少有一个参与者返回了NO，为1代表全部发回prepare，为-1代表参与者全挂了

    if(prepareResponse==0 || prepareResponse==1)//参与者全都发回prepared或至少有一个参与者返回NO
    {
        /*--------------第二阶段--------------------*/
        //向所有参与者发送COMMIT或ABORT请求
        string commitOrAbortRequestMsg;
        if(prepareResponse==0)
            commitOrAbortRequestMsg=MessageProcessor::getClusterAbortMsg(id);
        else
            commitOrAbortRequestMsg=MessageProcessor::getClusterCommitMsg(id);

        for(const int ps:participantSocket)
            if(ps>0)
                sendMessage(ps, commitOrAbortRequestMsg);

        //等待参与者发回DONE
        vector<string> response_split_msg;
        int commitResponse= waitForCommitOrAbort(participantSocket, aliveParticipantCount, commitOrAbortRequestMsg, response_split_msg);

        if(commitResponse==1 && prepareResponse==1)//收到了所有存活参与者的DONE反馈,且发出的是commit指令
            return MessageProcessor::getClientGetOrDelMsg(id,response_split_msg);
        else//在第二阶段中参与者全挂了，或收到了所有存活参与者的DONE反馈但是ABORT指令
            return MessageProcessor::getClientERRORMsg();//直接返回ERROR消息
    }
    else//在第一阶段中参与者全挂了
    {//直接返回ERROR消息
        return MessageProcessor::getClientERRORMsg();
    }
}

int Coordinator::waitForPrepared(vector<int> &participantSocket, int &aliveParticipantCount,const string &preparedRequestMsg){
    int maxfd=0;
    fd_set allFdSet;
    FD_ZERO(&allFdSet);
    for(int sockfd:participantSocket)
    {//初始化select函数要使用的两个参数
        if(sockfd>0)
        {
            FD_SET(sockfd,&allFdSet);
            maxfd=max(maxfd,sockfd);
        }
    }

    int preparedCount=0;
    int timeout=0;
    vector<int> response(participantCount,-1);//用于保存参与者返回结果，-1：未启用，0：NO，1：PREPARED
    while(aliveParticipantCount>0 && (preparedCount<aliveParticipantCount))
    {
        fd_set curFdSet=allFdSet;
        timeval tv{TIME_OUT_SECS,TIME_OUT_US};

        int nReady= select(maxfd+1,&curFdSet,nullptr,nullptr,&tv);

        if(nReady<0)//select函数发生错误
            return -1;
        else if(nReady==0)//超时
        {
            if(timeout==3)
            {//超过3次超时，将集合中所有参与者从系统移除
                for(int i=0;i<participantCount;i++)
                {
                    if(participantSocket[i]>0 && FD_ISSET(participantSocket[i],&allFdSet))
                    {
                        close(participantSocket[i]);
                        FD_CLR(participantSocket[i],&allFdSet);
                        participantSocket[i]=-1;
                        participantAddr[i].vaild=false;
                        aliveParticipantCount--;
                    }
                }
                timeout=0;
            }
            else
            {//向集合中的所有超时参与者重发prepared-request
                for(int sockfd:participantSocket)
                {
                    if(sockfd>0 && FD_ISSET(sockfd,&allFdSet))
                        sendMessage(sockfd,preparedRequestMsg);
                }
                timeout++;
            }
        }
        else//正常接收
        {
            timeout=0;//重置超时计数
            for(int i=0;i<participantCount&&nReady>0;i++)
            {
                if(participantSocket[i]>0 && FD_ISSET(participantSocket[i],&curFdSet))
                {
                    nReady--;
                    string participantMsg;
                    if(getMessage(participantSocket[i], participantMsg) <= 0)
                    {//连接已被断开或错误
                        close(participantSocket[i]);
                        FD_CLR(participantSocket[i],&allFdSet);
                        int reconfd=connectParticipant(i);//重连参与者
                        if(reconfd<0)
                        {//重连失败,将此参与者移除出系统
                            participantSocket[i]=-1;
                            participantAddr[i].vaild=false;
                            aliveParticipantCount--;
                        }
                        else
                        {//重连成功，则记录新的套接字，并重传prepared-request
                            participantSocket[i]=reconfd;
                            sendMessage(reconfd,preparedRequestMsg);
                            FD_SET(reconfd,&allFdSet);
                        }
                    }
                    else
                    {//正常接收，对参与者发回的数据进行解析
                        vector<string> partici_split_message;
                        if(MessageProcessor::parseClusterMsg(participantMsg, partici_split_message))
                        {//消息格式合法
                            if(partici_split_message[0]=="PREPARED")
                                response[i]=1;
                            else
                                response[i]=0;
                        }
                        else
                            response[i]=0;

                        //确认了一个参与者的回复，计数并移出监听集合
                        FD_CLR(participantSocket[i],&allFdSet);
                        preparedCount++;
                    }
                }
            }
        }
    }

    if(preparedCount==0)//参与者全挂了，错误情况
        return -1;

    if(checkParticipantPreparedResponse(response))//全prepared
        return 1;
    else//至少存在一个NO
        return 0;
}

int Coordinator::waitForCommitOrAbort(vector<int> &participantSocket, int &aliveParticipantCount,const string &requestMsg,vector<string> &response_split_msg) {
    int maxfd=0;
    fd_set allFdSet;
    FD_ZERO(&allFdSet);
    for(int sockfd:participantSocket)
    {//初始化select函数要使用的两个参数
        if(sockfd>0)
        {
            FD_SET(sockfd,&allFdSet);
            maxfd=max(maxfd,sockfd);
        }
    }

    int finishedCount=0;
    int timeout=0;
    while(aliveParticipantCount>0 && (finishedCount<aliveParticipantCount))
    {
        fd_set curFdSet=allFdSet;
        timeval tv{TIME_OUT_SECS,TIME_OUT_US};

        int nReady= select(maxfd+1,&curFdSet,nullptr,nullptr,&tv);

        if(nReady<0)//select函数发生错误
            return -1;
        else if(nReady==0)//超时,连接未断但参与者没响应
        {
            if(timeout==3)
            {//超过3次超时，将集合中所有参与者从系统移除
                for(int i=0;i<participantCount;i++)
                {
                    if(participantSocket[i]>0 && FD_ISSET(participantSocket[i],&allFdSet))
                    {
                        close(participantSocket[i]);
                        FD_CLR(participantSocket[i],&allFdSet);//从原描符集合中删除
                        participantSocket[i]=-1;
                        participantAddr[i].vaild=false;
                        aliveParticipantCount--;
                    }
                }
                timeout=0;
            }
            else
            {//向集合中的所有超时参与者重发prepared-request
                for(int sockfd:participantSocket)
                {
                    if(sockfd>0 && FD_ISSET(sockfd,&allFdSet))
                        sendMessage(sockfd,requestMsg);
                }
                timeout++;
            }
        }
        else//正常接收
        {
            timeout=0;//重置超时计数
            for(int i=0;i<participantCount&&nReady>0;i++)
            {
                if(participantSocket[i]>0 && FD_ISSET(participantSocket[i],&curFdSet))
                {
                    nReady--;//就绪连接数-1
                    string participantMsg;
                    if(getMessage(participantSocket[i], participantMsg) <= 0)
                    {//连接已被断开或错误
                        close(participantSocket[i]);
                        FD_CLR(participantSocket[i],&allFdSet);//从原描符集合中删除
                        int reconfd=connectParticipant(i);//尝试重连参与者
                        if(reconfd<0)
                        {//重连失败,将此参与者移除出系统
                            participantSocket[i]=-1;
                            participantAddr[i].vaild=false;
                            aliveParticipantCount--;
                        }
                        else
                        {//重连成功，则记录新的套接字，并重传commit或abort
                            participantSocket[i]=reconfd;
                            sendMessage(reconfd,requestMsg);
                            FD_SET(reconfd,&allFdSet);
                        }
                    }
                    else
                    {//正常接收，对参与者发回的数据进行解析
                        vector<string> partici_split_message;
                        //这里假定参与者回复的消息一定是合法的，处理情况不完全，参与者发回的数据可能不合法，待完善.......
                        if(MessageProcessor::parseClusterMsg(participantMsg, partici_split_message))
                        {
                            if(finishedCount==0)//如果是收到的是第一个参与者的数据，则记录
                                response_split_msg=partici_split_message;
                        }

                        //确认了一个参与者的回复，计数并移出监听集合，至此可以将与此参与者的连接断开了
                        FD_CLR(participantSocket[i],&allFdSet);
                        finishedCount++;

                        close(participantSocket[i]);
                        participantSocket[i]=-1;
                    }
                }
            }
        }
    }

    if(finishedCount==0)//参与者全挂了,没一个完成
        return -1;
    else
        return 1;
}

int Coordinator::connectAllParticipants(vector<int> &participantSocket) {
    int connectSuccessCount=0;
    for(int i=0;i<participantCount;i++)
    {
        if(participantAddr[i].vaild)
        {
            //申请一个新套接字
            int sockfd= socket(AF_INET,SOCK_STREAM,0);
            //设置目标参与者的ip和端口
            sockaddr_in participantaddr=MessageProcessor::getSockAddr(participantAddr[i].ip,participantAddr[i].port);

            //设置本机要与参与者通信的ip,不用指定端口
            sockaddr_in communicateAddr=MessageProcessor::getSockAddr(curAddr.ip,-1);

            //进行绑定，指定与参与者连接的ip
            if(bind(sockfd,(sockaddr*)&communicateAddr,sizeof(communicateAddr))<0)
                cout<<"Wrong bind in connecting participant!"<<endl;

            int connectCount=0;
            for(connectCount=0;connectCount<3;connectCount++)//最多尝试3次连接
            {
                if(connect(sockfd,(sockaddr*)&participantaddr,sizeof(participantaddr))<0)
                {//连接失败，睡眠1s，然后再次尝试
                    //后来这里改成不睡眠，否则时间过长会通不过测试脚本
                    //sleep(1);
                    continue;
                }
                else
                {//连接成功，直接记录并跳出循环
                    participantSocket[i]=sockfd;
                    connectSuccessCount++;
                    break;
                }
            }
            if(connectCount==3)//连接3次连不上视作节点已当机
                participantAddr[i].vaild=false;
        }
    }
    return connectSuccessCount;
}

int Coordinator::connectParticipant(int i) {
    if(!participantAddr[i].vaild)
        return -1;

    //申请一个新套接字
    int sockfd= socket(AF_INET,SOCK_STREAM,0);

    //设置目标参与者的ip和端口
    sockaddr_in participantaddr=MessageProcessor::getSockAddr(participantAddr[i].ip,participantAddr[i].port);

    //设置本机要与参与者通信的ip,不用指定端口
    sockaddr_in communicateAddr=MessageProcessor::getSockAddr(participantAddr[i].ip,-1);

    //进行绑定，指定与参与者连接的ip
    if(bind(sockfd,(sockaddr*)&communicateAddr,sizeof(communicateAddr))<0)
        cout<<"Wrong bind in connecting participant!"<<endl;

    int connectCount=0;
    for(connectCount=0;connectCount<3;connectCount++)
    {
        if(connect(sockfd,(sockaddr*)&participantaddr,sizeof(participantaddr))<0)
        {//连接失败，睡眠1s，然后再次尝试
            //后来这里改成不睡眠，否则时间过长会通不过测试脚本
            //sleep(1);
            continue;
        }
        else
            return sockfd;
    }
    //进行到这里说明3次连接都失败了
    participantAddr[i].vaild=false;
    return -1;
}

int Coordinator::getRandomID() {
    uniform_int_distribution<int> dis(0,9999);
    return dis(randev);
}

int Coordinator::getMessage(int connfd, string &data) const {
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

int Coordinator::sendMessage(int connfd, const string &data) {
    if(data.empty())
        return 0;

    if(send(connfd,data.c_str(),data.size(),0)<0)
        return -1;

    return 1;
}

bool Coordinator::checkParticipantPreparedResponse(const vector<int> &response) {
    if(response.empty())
        return false;

    for(const int resp:response)
    {
        if(resp==0)
            return false;
    }
    return true;
}
