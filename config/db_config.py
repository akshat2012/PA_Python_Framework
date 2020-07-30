mysql = {}
presto = {}

mysql['dev'] = {'host': "52.76.148.206",
               'user': "akshat",
               'password': "<pwd>",
               'db': '<db>'
        }

mysql['qa'] = {'host': "52.76.148.206",
               'user': "akshat",
               'password': "<pwd>",
               'db': '<db>'
        }

mysql['prod'] = {'host': "172.31.31.149",
                 'user': "freq_report",
                 'password': "<pwd>",
                 'db': '<db>'
        }


presto['dev'] = {'host' : "127.0.0.1",
                    'port' : "8080",
                    'username' : "root",
                    'catalog' : "hive",
                    'schema' : "default",
                    'password' : "PRESTO_PASSWORD",
                    'cert_path' : "CONFIG_PRESTO_CER"
                }

def getmysqlconfig(conf):
    return mysql[conf]

def getprestoconfig(conf):
    return presto(conf)
