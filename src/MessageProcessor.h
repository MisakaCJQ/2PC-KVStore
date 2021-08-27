/*此类用于解析和封装客户端请求消息
 * 解析和封装集群间的请求和应答ixaoxi
 * 解析配置文件
 * 封装套接字地址结构
*/
#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <string>
#include <sstream>
#include <netinet/in.h>
#include <arpa/inet.h>

#ifndef KVSTORE_MESSAGEPROCESSOR_H
#define KVSTORE_MESSAGEPROCESSOR_H

class MessageProcessor {
public:
    static bool parseClientMsg(const std::string &str, std::vector<std::string> &split_message);
    static bool parseClusterMsg(const std::string &str, std::vector<std::string> &split_message);
    static std::vector<std::vector<std::string>> parseConfigFile(const std::string &path);
    static std::string getClusterPrepareRequestMsg(int id, const std::vector<std::string> &split_message);
    static std::string getClusterCommitMsg(int id);
    static std::string getClusterAbortMsg(int id);
    static std::string getClusterPrepareMsg(int id,const std::string &method);
    static std::string getClusterDoneMsg(int id,const std::string &method,const std::string &additionStr);
    static std::string getClientOKMsg();
    static std::string getClientERRORMsg();
    static std::string getClientGetOrDelMsg(int id,const std::vector<std::string> &split_message);
    static sockaddr_in getSockAddr(const std::string &ip,int port);
private:
    static void cutStringBySpace(const std::string &str,std::vector<std::string> &split_str);
};


#endif //KVSTORE_MESSAGEPROCESSOR_H
