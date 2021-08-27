#include "MessageProcessor.h"

using namespace std;

bool MessageProcessor::parseClientMsg(const string &str, vector<string> &split_message) {
    if(str.empty())
        return false;
    split_message.clear();//确保保存结果的vector为空
    stringstream messageSS(str);
    string temp;
    while(getline(messageSS,temp))
    {
        if(temp[0]!='*' && temp[0]!='$')
        {//这里只提取含有效字符串的行,丢弃表明字数的行
            temp.pop_back();//将尾部的\r删掉
            split_message.push_back(temp);
        }
    }
    unsigned int lineNum=split_message.size();
    if(lineNum<2 || (split_message[0]!="SET" && split_message[0]!="GET" && split_message[0]!="DEL"))
        return false;

    if(split_message[0]=="SET" && lineNum>3)
    {//对SET操作进行处理，将value部分合并为一个字符串
        for(int i=3;i<lineNum;i++)
        {
            split_message[2]+=' ';
            split_message[2]+=split_message[i];
        }
        //合并完后将剩下的多余value分词字符串删除
        for(int i=3;i<lineNum;i++)
            split_message.pop_back();
    }
    return true;
}

bool MessageProcessor::parseClusterMsg(const string &str, vector<string> &split_message) {
    if(str.empty())
        return false;
    split_message.clear();//确保保存结果的vector为空
    stringstream messageSS(str);
    string temp;
    while(getline(messageSS,temp))
    {
            temp.pop_back();//将尾部的\r删掉
            split_message.push_back(temp);
    }
    unsigned int lineNum=split_message.size();
    if(lineNum<2)
        return false;
    return true;
}

vector<vector<string>> MessageProcessor::parseConfigFile(const string &path) {
    vector<vector<string>> split_file_data;

    ifstream fin(path);
    if(!fin)
        return split_file_data;
    string str;

    while(getline(fin,str))
    {
        if(str.empty() || str[0]=='!')//为空行或注释行则跳过
            continue;
        vector<string> split_str;
        cutStringBySpace(str,split_str);
        if(split_str[0]=="coordinator_info" || split_str[0]=="participant_info")
        {
            auto pos=split_str[1].find(':');
            if(pos!=string::npos)
            {//切分地址和端口字段
                split_str.push_back(split_str[1].substr(pos+1));
                split_str[1]=split_str[1].substr(0,pos);
            }
        }
        split_file_data.push_back(split_str);
    }

    return split_file_data;
}

string MessageProcessor::getClusterPrepareRequestMsg(int id, const vector<string> &split_message) {
    string msg="PREPARED-REQUEST\r\n" + to_string(id) + "\r\n";//加上头部
    for(const string &str:split_message)
    {
        msg+=str;
        msg+="\r\n";
    }
    return msg;
}

string MessageProcessor::getClusterCommitMsg(int id) {
    string msg="COMMIT\r\n" + to_string(id) + "\r\n";
    return msg;
}

string MessageProcessor::getClusterAbortMsg(int id) {
    string msg="ABORT\r\n" + to_string(id) + "\r\n";
    return msg;
}

string MessageProcessor::getClusterPrepareMsg(int id, const string &method) {
    return "PREPARED\r\n" + to_string(id) + "\r\n" + method + "\r\n";
}

string MessageProcessor::getClientOKMsg() {
    return "+OK\r\n";
}

string MessageProcessor::getClientERRORMsg() {
    return "-ERROR\r\n";
}

string MessageProcessor::getClientGetOrDelMsg(int id, const vector<string> &split_message) {
    auto lineNum=split_message.size();
    //如果不是正好4行数据
    if(lineNum!=4)
        return getClientERRORMsg();

    if(split_message[2]=="GET")
    {//GET方法的返回数据
        vector<string> split_value;
        cutStringBySpace(split_message[3],split_value);
        string retMsg="*"+to_string(split_value.size())+"\r\n";
        for(const string& word:split_value)
        {
            retMsg+="$" + to_string(word.size()) + "\r\n";
            retMsg+=word+"\r\n";
        }
        return retMsg;
    }
    else if(split_message[2]=="DEL")
        return ":" + split_message[3] + "\r\n";
    else//不是GET也不是DEL，说明数据错误
        return getClientERRORMsg();
}

void MessageProcessor::cutStringBySpace(const string &str, vector<string> &split_str) {
    split_str.clear();//将用于保存结果的数组清空
    if(str.empty())
        return ;

    stringstream ss(str);
    string word;

    while(ss>>word)
        split_str.push_back(word);

}

string MessageProcessor::getClusterDoneMsg(int id, const string &method, const string &additionStr) {
    if(!additionStr.empty())
        return "DONE\r\n" + to_string(id) + "\r\n" + method + "\r\n" + additionStr + "\r\n";
    else
        return "DONE\r\n" + to_string(id) + "\r\n" + method + "\r\n";
}

sockaddr_in MessageProcessor::getSockAddr(const string &ip, int port) {
    sockaddr_in sockAddr{};
    memset(&sockAddr, 0, sizeof(sockAddr));
    sockAddr.sin_family=AF_INET;//设定ipv4协议
    inet_pton(AF_INET,ip.c_str(),&sockAddr.sin_addr);//设定ip地址
    if(port>0)//如果存在端口，则设置端口
        sockAddr.sin_port= htons(port);

    return sockAddr;
}



