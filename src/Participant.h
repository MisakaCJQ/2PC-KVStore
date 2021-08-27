/*
 * 2PC协议参与者类定义
 */
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <string>
#include <utility>
#include <vector>
#include <utility>
#include <cstring>
#include <random>
#include <ctime>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "MessageProcessor.h"

#ifndef KVSTORE_PARTICIPANT_H
#define KVSTORE_PARTICIPANT_H

class Participant {
public:
    Participant(std::string _ip,int _port):ip(std::move(_ip)),port(_port),phase(1),id(-1){}
    void startup();
private:
    int handleConnection(int connfd,sockaddr_in cliaddr);
    std::string handlePreparePhase(const std::vector<std::string> &split_message);//一阶段处理
    std::string handleCommitOrAbortPhase(const std::vector<std::string> &split_message);//二阶段处理
    int handleExceptionMessage(const std::vector<std::string> &split_message,std::string &responseMsg);//放弃使用
    int getMessage(int connfd,std::string &data) const;
    int sendMessage(int connfd,const std::string &data);
    void rollBack();
private:
    std::string ip;
    int port;
    int phase;//用于标记所处的阶段
    int id;//用于记录事务id
    std::string lastResponseMsg;//记录协调者请求内容，便于回滚以及重传时使用
    const unsigned int MAX_BUF_SIZE=512;
    std::unordered_map<std::string,std::string> db;
    std::vector<std::vector<std::string>> operationLog;//操作日志
    std::vector<std::vector<std::string>> rollbackLog;//回滚日志
};


#endif //KVSTORE_PARTICIPANT_H
