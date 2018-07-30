#-*- coding:utf-8 -*-

import MySQLdb
import time
import re
import paramiko
import getopt
import sys

host = "you'r host ip"
user = "you'r hostname"
passwd = "you'r host password"

ssh_host = host
ssh_port = 22
ssh_username = "root"
ssh_password = "feixun"
logPath = "D:/back_MySQL_log.txt"


class MySQL_Backup():
    
    ###############################################################
    ##初始化数据库及时间相关
    def __init__(self, start_time, end_time):
        ###########################################################
        ## 生成日志文件
        self.logWriter  = open(logPath, "a")
        
        self.start_time = int(time.mktime(time.strptime(start_time, "%Y%m%d")))
        self.end_time = int(time.mktime(time.strptime(end_time, "%Y%m%d")))
        print "backup from %s to %s"%(start_time, end_time)
        self.logWriter.write("backup from %s to %s\r\n"%(start_time, end_time))

        recover_time = time.strftime("%Y_%m", time.gmtime(self.end_time))
        ###########################################################
        ## 定义备份与恢复路径
        self.root_path = "/home/HistoryDir/"+recover_time
        
        ###########################################################
        ## 需要恢复到的数据表
        self.restore_tb = recover_time

        ###########################################################
        ## 建立数据库及SSH连接
        self.my = MySQLdb.connect(host=host, user = user, passwd = passwd, port = 3306)
        self.cur = self.my.cursor()

        self.restore_client = paramiko.SSHClient()
        self.restore_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.restore_client.connect(hostname = ssh_host, port = ssh_port, username = ssh_username, password = ssh_password)
        stdin, stdout, stderr = self.restore_client.exec_command("ls %s"%self.root_path)

        ###########################################################
        ## 判断目录及数据表是否存在，不存在则创建
        stdout_msg = stdout.read()
        stderr_msg = stderr.read()
        if (stderr_msg != '') and ('ls' in stderr_msg):
            print "%s"%stderr_msg
            self.writeLog("%s"%stderr_msg)
            
            print "Create dir %s"%self.root_path
            self.writeLog("Create dir %s"%self.root_path)
            
            stdin, stdout, stderr = self.restore_client.exec_command("mkdir %s"%self.root_path)
            stderr_msg = stderr.read()
            if stderr_msg != '':
                print "create dir %s failed!"%self.root_path
                self.writeLog("create dir %s failed!"%self.root_path)
            else:
                stdin, stdout, stderr = self.restore_client.exec_command("chown mysql:mysql %s"%self.root_path)
        try:        
            stdin, stdout, stderr =  self.restore_client.exec_command("ls %s"%self.root_path)
            has_file = stdout.read()
            self.has_file = has_file.split("\n")
            print self.has_file
        except:
            pass
        
        self.cur.execute("use history_zabbix")
        self.cur.execute("show tables;")
        ret_tables = self.cur.fetchall()
        table_List = list(ret_tables)
        table_list = []
        for t in table_List:
            table_list.append(list(t)[0])
            
        if self.restore_tb not in table_list:
            print "There is no table named %s"%self.restore_tb
            self.writeLog("There is no table named %s"%self.restore_tb)
            
            print "Create table %s"%self.restore_tb
            self.writeLog("Create table %s"%self.restore_tb)
            
            self.cur.execute('''CREATE TABLE `%s` (
`itemid` bigint(20) unsigned NOT NULL,
`clock` int(11) NOT NULL DEFAULT '0',
`value` bigint(20) unsigned NOT NULL DEFAULT '0',
KEY `history_uint_1` (`itemid`,`clock`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;'''%self.restore_tb)
    ###########################################################
    ## 日志记录
    def writeLog(self, message):
        self.logWriter.write(message+"\r\n")
        self.logWriter.flush()
    ###############################################################
    ## 释放接口
    def __del__(self):
        self.logWriter.close()
    ###############################################################
    ## 获取host_name 和 网卡名 的匹配值
    def getHostName(self):
        getHostName_start_time = time.time()
        getHostName_start_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(getHostName_start_time))
        print "getHostName - start at : %s"%getHostName_start_at
        self.writeLog("getHostName - start at : %s"%getHostName_start_at)
        
        self.cur.execute('''SELECT host_name,networkcard_name FROM test.terminal where host_name!='[]' and networkcard_name !='[]' ORDER BY host_name;''')
        host_name = self.cur.fetchall()
        print len(host_name)
        self.writeLog("get Host Length:	        %d"%len(host_name))
        host_name = sorted(list(host_name))
        test_bed_pc = []
        for host in host_name:
            test_bed_tmp = re.findall('\d+',host[0].split("-")[2])[0]
            test_pc_tmp = host[0].split("-")[3]
            test_bed_pc.append((test_bed_tmp,test_pc_tmp,host))
        
    
        getHostName_end_time = time.time()
        getHostName_end_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(getHostName_end_time))
        print "getHostName - end at : %s"%getHostName_end_at
        self.writeLog("getHostName - end at :   %s"%getHostName_end_at)
        
        seconds = getHostName_end_time - getHostName_start_time
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        print "getHostName Duration %02d:%02d:%02d"%(h, m, s)
        self.writeLog("getHostName Duration     %02d:%02d:%02d"%(h, m, s))
        
        return test_bed_pc
    ###############################################################
    ## 依据host_name 和 网卡名 的匹配值,获取itemid
    def getItemID(self, test_bed_pc):
        getItemID_start_time = time.time()
        getItemID_start_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(getItemID_start_time))
        print "getItemID - start at :   %s"%getItemID_start_at
        self.writeLog("getItemID - start at :   %s"%getItemID_start_at)
        
        itemid_all = []
        for host in test_bed_pc:
            self.cur.execute('''SELECT itemid from zabbix.items WHERE hostid =(SELECT hostid from zabbix.hosts WHERE host='%s') 
                        AND key_ IN (concat('net.if.in','%s'),
                        concat('net.if.out','%s'));'''%(host[2][0],host[2][1],host[2][1]))
            time.sleep(0.5)
            ret = self.cur.fetchall()
            if ret == ():
                continue
            else:
                ret_1 = int(ret[0][0])
                ret_2 = int(ret[1][0])
                itemid_all.append((host[0],host[1],ret_1,ret_2))

        getItemID_end_time = time.time()
        getItemID_end_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(getItemID_end_time))
        print "getItemID - end at : %s"%getItemID_end_at
        self.writeLog("getItemID - end at :     %s"%getItemID_end_at)
        
        seconds = getItemID_end_time - getItemID_start_time
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        print "getItemID Duration %02d:%02d:%02d"%(h, m, s)
        self.writeLog("getItemID Duration       %02d:%02d:%02d"%(h, m, s))
        
        return itemid_all
    ###############################################################
    ## 获取固定时间段内测试项
    def getTestCase(self, itemid_all):
        getTestCase_start_time = time.time()
        getTestCase_start_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(getTestCase_start_time))
        print "getTestCase - start at : %s"%getTestCase_start_at
        self.writeLog("getTestCase - end at :   %s"%getTestCase_start_at)
        
        print "get test case fome %s to %s"%(time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(self.start_time)),
                                             time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(self.end_time)))
        self.writeLog("get test case fome %s to %s"%(time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(self.start_time)),
                                             time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(self.end_time))))
        ret_rows = self.cur.execute("select test_bed_id,start_time,end_time from test.test where start_time between %s and %s;"%(self.start_time, self.end_time))
        test_items =  []
        for i in range(ret_rows):
            ret_fet = self.cur.fetchone()
            print ret_fet
            try:
                test_items.append((int(ret_fet[0]),int(ret_fet[1]),int(ret_fet[2])))
            except ValueError:
                print "itemid wrong"
            except:
                print "something else wrong"
        test_items = sorted(test_items)

        getTestCase_end_time = time.time()
        getTestCase_end_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(getTestCase_end_time))
        print "getTestCase - end at : %s"%getTestCase_end_at
        self.writeLog("getTestCase - end at :   %s"%getTestCase_end_at)
        
        seconds = getTestCase_end_time - getTestCase_start_time
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        print "GetTestCase Duration %02d:%02d:%02d"%(h, m, s)
        self.writeLog("GetTestCase Duration     %02d:%02d:%02d"%(h, m, s))
        
        return test_items
    ###############################################################
    ##开始备份
    def backup(self, test_items, itemid_all):
        backup_start_time = time.time()
        backup_start_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(backup_start_time))
        print "Backup - start at : %s"%backup_start_at
        self.writeLog("Backup - start at :      %s"%backup_start_at)
        cnt = 0
        root_path = self.root_path
        print "备份路径 ： %s"%root_path
        self.writeLog("备份路径 ：               %s"%root_path)
        
        for test_id in test_items:
            for item_id in itemid_all:
                if item_id[0] == str(test_id[0]):
                    
                    file_cnt = str(cnt).zfill(3)
                    item_1 = item_id[2]
                    item_2 = item_id[3]
                    start_time = test_id[1]
                    end_time = test_id[2]
                    
                    file_now = ('%s-%s-%s-%s.txt')%(str(item_id[0]).zfill(3), test_id[1], str(item_id[1]).zfill(2), item_1)
                    ## 检测该项是否已经做过备份
                    if file_now in self.has_file: 
                        print "file_now %s is already exist!!!"%file_now
                    else:
                        cnt += 1
                        sql_s = ('''SELECT b.itemid,b.clock,b.value, b.ns into outfile '%s/%s' fields terminated by ','
            FROM zabbix.items a, zabbix.history_uint b WHERE a.itemid = b.itemid and b.itemid in (%s,%s)
                           AND clock >= %s AND clock <= %s;''')%(root_path, file_now, item_1, item_2, start_time, end_time)

                        print sql_s
                        self.writeLog(sql_s)
                        try:
                            self.cur.execute(sql_s)    
                        except Exception,e:
                            print e
                            return -1
                    time.sleep(1)

        backup_end_time = time.time()
        backup_end_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(backup_end_time))
        print "Backup - end at : %s"%backup_end_at
        self.writeLog("Backup - end at :        %s"%backup_end_at)
        
        seconds = backup_end_time - backup_start_time
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        print "Backup Duration %02d:%02d:%02d"%(h, m, s)
        self.writeLog("Backup Duration          %02d:%02d:%02d"%(h, m, s))
        return cnt
    ###############################################################
    ##开始恢复
    def restore(self):
        restore_start_time = time.time()
        restore_start_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(restore_start_time))
        print "restore - start at : %s"%restore_start_at
        self.writeLog("restore - start at :     %s"%restore_start_at)
        
        root_path = self.root_path
        restore_tb = self.restore_tb
        stdin, stdout, stderr = self.restore_client.exec_command("ls %s"%root_path)
        file_list = stdout.read().split("\n")
        self.cur.execute("use zabbix")
        
        for file_name in file_list:
            if file_name != '':
                print file_name
                self.writeLog(file_name)
                if file_name in self.has_file:
                    print "file %s has been restored, this file will be passed"
                else:
                    s = "load data infile '%s/%s' into table %s FIELDS TERMINATED BY ','"%(root_path, file_name, restore_tb)
                    print s
                    self.writeLog(s)
                    self.cur.execute(s)
                    self.my.commit()
                
        restore_end_time = time.time()
        restore_end_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(restore_end_time))
        print "restore - end at : %s"%restore_end_at
        self.writeLog("restore - end at :       %s"%restore_end_at)
        
        seconds = restore_end_time - restore_start_time
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        print "restore Duration %02d:%02d:%02d"%(h, m, s)
        self.writeLog("restore Duration         %02d:%02d:%02d"%(h, m, s))
    ###############################################################
    ##主控程序
    def mainControl(self):
        mainControl_start_time = time.time()
        mainControl_start_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(mainControl_start_time))
        print "mainControl - start at :         %s"%mainControl_start_at
        self.writeLog("*************************************************\r\n")
        self.writeLog("mainControl - start at : %s"%mainControl_start_at)
        test_bed_pc = self.getHostName()
        print test_bed_pc
        itemid_all = self.getItemID(test_bed_pc)
        print itemid_all
        test_items = self.getTestCase(itemid_all)
        
        cnt = self.backup(test_items, itemid_all)

        if cnt == -1:
            print "Error"
        elif cnt == 0:
            print "this is no data need to be backuped"
        else:
            mb.restore()
            
        mainControl_end_time = time.time()
        mainControl_end_at = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(mainControl_end_time))
        print "mainControl - end at : %s"%mainControl_end_at
        seconds = mainControl_end_time - mainControl_start_time
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        print "mainControl Duration %02d:%02d:%02d"%(h, m, s)
        self.writeLog("mainControl - end at :   %s"%mainControl_end_at)
        self.writeLog("mainControl Duration     %02d:%02d:%02d"%(h, m, s))
        self.writeLog("*************************************************\r\n")
###############################################################
##主函数判断
if __name__ == "__main__":
    end_time_tuple = time.localtime()
    end_time = time.strftime("%Y%m", end_time_tuple)
    end_time = end_time+"01"
    tm_year = end_time_tuple.tm_year
    tm_mon = end_time_tuple.tm_mon
    if tm_mon > 1:
        tm_mon = tm_mon - 1
    else:
        tm_mon = 12
        tm_year = tm_year - 1
    start_time = str(tm_year)+str(tm_mon).zfill(2)+"01"
    print "start_time %s"%start_time
    print "end_time %s"%end_time
##    start_time = "20171101"
##    end_time = "20171129"
    mb = MySQL_Backup(start_time, end_time)
    mb.mainControl()
