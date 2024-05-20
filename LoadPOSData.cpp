# define _GLIBCXX_USE_CXX11_ABI 0
#include <iostream>
#include <fstream>
#include <vector>
#include <occi.h>
#include <sstream>
#include <unistd.h>

#include "tinyxml2.h"

using namespace oracle::occi;
using namespace tinyxml2;
using namespace std;

struct field {
    string de;
    int order;
    string type;
    int start;
    int length;
    string padType;
    string padStr;
};

string getXMLValue(XMLNode* xmlNode, const char* tagName) {
    XMLElement* xmlElem = xmlNode->FirstChildElement(tagName);
    if (xmlElem == NULL) {
        return "";
    } else {
        const char* value = xmlElem->GetText();
        if (value == NULL) {
            return "";
        } else {
            return value;
        }
    }
}

int getFieldList(vector<field> &fieldList, const char* xmlFileName) {
    cout << "Start parsing." << endl;
    XMLDocument xmlDoc;
    xmlDoc.LoadFile(xmlFileName);
    XMLHandle xmlDocHandle(&xmlDoc);

    string length;
    int start = 0, intLength;
    for( XMLNode* xmlField = xmlDocHandle.FirstChildElement( "FIELDS" ).FirstChildElement("FIELD").ToNode(); xmlField; xmlField = xmlField->NextSibling() ) {
        field tmpField;
        tmpField.de = getXMLValue(xmlField,"DE");
        tmpField.order = atoi(getXMLValue(xmlField,"ORDER").c_str());
        tmpField.type = getXMLValue(xmlField,"TYPE");
        tmpField.start = start;
        length = getXMLValue(xmlField,"LENGTH");
        if (length == ""){
            cerr << "Could not get LENGTH." << endl;
            fieldList.clear();
            return -1;
        }
        intLength = atoi(length.c_str());
        tmpField.length = intLength;
        tmpField.padType = getXMLValue(xmlField,"PAD_TYPE");
        tmpField.padStr = getXMLValue(xmlField,"PAD_STR");
        fieldList.push_back(tmpField);
        
        start = start + intLength;
    }
    int listSize = fieldList.size();
    int check = 1;
    int maxLength = 0;
    int totalLength = 0;
    cout << "Got " << listSize << " field definitions from XML." << endl; 
    for (vector<field>::iterator it = fieldList.begin(); it != fieldList.end(); ++it) {
        if (check == (*it).order) {
            check++;
        } else {
            cerr << "Not all fields are descripted in xml. Check ORDER!" << endl;
            return -1;
        }
        if ((*it).length > maxLength && (*it).de != "") {
            maxLength = (*it).length;
        }
        totalLength = totalLength + (*it).length;
    }
    cout << "Maximum length:" << maxLength << endl;
    cout << "Total length:" << totalLength << endl;
    cout << "End parsing." << endl;
    return 0;
}

string getFieldValue(string &line, field &f) {
    int endPos = f.start + f.length;
    if (endPos > line.length()){
        cerr << "End position bigger than string length." << endl;
        cerr << line;
        cerr << "start:" << f.start << " length:" << f.length << " end:" << endPos << endl;
        return "";
    }
    string fieldValue = "";
    size_t found;
    fieldValue = line.substr(f.start,f.length);
    if (f.padType == "RIGHT") {
        found=fieldValue.find_last_not_of(f.padStr);
        if (found!=string::npos) {
            fieldValue.erase(found+1);
        } else {
            fieldValue.clear();
        }
    } else {
        found=fieldValue.find_first_not_of(f.padStr);
        if (found!=string::npos) {
            fieldValue = fieldValue.substr(found);
        } else {
            fieldValue.clear();
        }
    }
    return fieldValue;
}

string getInsertStatement(vector<field> &fieldList) {
    cout << "Build SQL insert statement" << endl;
    string parameterNames = "";
    string parameterValues = "";
    int intBindNr = 1;
    for (vector<field>::iterator it = fieldList.begin(); it != fieldList.end(); ++it) {
        if ((*it).de.length()>0) {
            parameterNames = parameterNames + (*it).de + ", ";
            ostringstream bindNr;
            bindNr << intBindNr;
            parameterValues = parameterValues + ":" + bindNr.str() + ", ";
            intBindNr++;
        }
    }
    
    parameterNames = parameterNames.erase(parameterNames.length()-2);
    
    parameterValues = parameterValues.erase(parameterValues.length()-2);
    
    string sql = "INSERT INTO POS_TRANS (" + parameterNames + ") VALUES (" + parameterValues + ")";
    return sql;
}

void writeToLogFile(string text) {
    ofstream oFile;
    oFile.open("load_data_pos_log.txt", ios::app);
    if (oFile.is_open()) {
        oFile << text << endl;
        oFile.close();
    } else {
        cout << "Unable to write to log file" << endl;
    }
    return;
}

int main() {
    const string user = "user";
    const string pass = "password";
    const string conString = "//dbserver:1521/PDB1";
    const int maxIterations = 100000;
    const int rowsToSkip = 0;
    const int rowsToProcess = 0;

    Environment* const env = Environment::createEnvironment(Environment::DEFAULT);
    try {

        vector<field> fieldList;
        if (getFieldList(fieldList,"cfg/POS.xml")) {
            cerr << "Could not get field configuration from xml." << endl;
        }
        field tranDateTime;
        tranDateTime.de = "TRAN_DATETIME";
        tranDateTime.type = "datetime";
        tranDateTime.length = 0;
        fieldList.push_back(tranDateTime);

        Connection* const con = env->createConnection(user, pass, conString);
        cout << "Connected to DB" << endl;
        string insertStatement = getInsertStatement(fieldList);
        
        Statement *stmt = con->createStatement(insertStatement);
        stmt->setMaxIterations(maxIterations);
        
        int bindNr = 1;
        int minimumLineLength = 0;
        for (vector<field>::iterator it = fieldList.begin(); it != fieldList.end(); ++it) {
            if ((*it).de.length()>0 && (*it).length>0) {
                stmt->setMaxParamSize(bindNr, (*it).length);
                bindNr++;
            }
            minimumLineLength = minimumLineLength + (*it).length;
        }
        cout << "Minimum line length:" << minimumLineLength << endl;
        
        string line;
        ifstream dataFile("pos.txt");
        if (dataFile.is_open()) {
            int iterationCount = 0;
            int rowsProcessed = 0;
            int rowsInserted = 0;
            while (dataFile.good()) {
                getline (dataFile,line);
                rowsProcessed++;
                if (rowsProcessed <= rowsToSkip) {
                    //skip
                } else if (line.substr(0,2) != "DR" && line.substr(0,2) != "dr") {
                    cout << "Skip this line. Line starts with [" << line.substr(0,2) << "]." << endl;
                } else if (line.length() < minimumLineLength) {
                    cout << "Line too short. Will write it to output file. Line length :" << line.length() << endl;
                    ofstream errorFile;
                    errorFile.open("skipped_pos_lines.txt", ios::app);
                    if (errorFile.is_open()) {
                        errorFile << line <<endl;
                        errorFile.close();
                    } else {
                        cout << "Unable to open skipped_pos_lines.txt file" << endl;
                    }
                } else {
                    if(iterationCount > 0){
                        stmt->addIteration();
                    }
                    bindNr = 1;
                    string tranDate, tranTime;
                    for (vector<field>::iterator it = fieldList.begin(); it != fieldList.end(); ++it) {
                        if ((*it).de.length()>0 && (*it).type == "string") {
                            if ((*it).type == "string") {
                                stmt->setString(bindNr, getFieldValue(line,*it));
                                if ((*it).de == "TRAN_DAT"){
                                    tranDate = getFieldValue(line,*it);
                                }
                                if ((*it).de == "TRAN_TIM"){
                                    tranTime = getFieldValue(line,*it);
                                }
                            }
                            bindNr++;
                        }
                        if ((*it).de == "TRAN_DATETIME") {
                            Date oraDate;
                            oraDate.fromText(("20"+tranDate+tranTime.substr(0,6)),"YYYYMMDDHH24MISS","",env);
                            stmt->setDate(bindNr, oraDate);
                            bindNr++;
                        }

                    }
                    iterationCount++;
                    rowsInserted++;
                    if (iterationCount == maxIterations) {
                        stmt->executeUpdate();
                        con->commit();
                        ostringstream text;
                        text << "Rows inserted:" << rowsInserted << " Rows processed:" << rowsProcessed;
                        writeToLogFile(text.str());
                        iterationCount = 0;
                    }
                    if (rowsInserted % 1000000 == 0){
                        cout << "Sleep for 10 seconds." << endl;
                        sleep(10);
                        cout << "Continue." << endl;
                    }
                }
                if (rowsProcessed == rowsToProcess && rowsToProcess > 0) {
                    break;
                }
            }
            if(iterationCount > 0){
                stmt->executeUpdate();
                con->commit();
            }
            ostringstream text;
            text << "Finished. Rows inserted:" << rowsInserted << " Rows processed:" << rowsProcessed;
            writeToLogFile(text.str());
            con->terminateStatement(stmt);
            dataFile.close();
        } else {
            cerr << "Unable to open file" << endl;
        }
        
        cout << "Closing connection." << endl;
        env->terminateConnection(con);
    } catch (SQLException ea) {
        cerr << ea.what();
    }
    Environment::terminateEnvironment(env);
    return 0;
}
