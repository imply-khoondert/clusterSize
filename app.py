from flask import Flask, render_template, request
import yaml
import math


app = Flask(__name__)


def roundUpDiv(a,b):
    return int((a + (-a % b)) // b)


def roundUpToEven(f):
    return math.ceil(f / 2.) * 2


@app.route('/')
def main():
    return render_template('app.html')


@app.route('/send', methods=['POST'])
def send(sum=sum):
    if request.method == 'POST':
        cpu = request.form['cpu']
        mem = request.form['mem']
        disk = request.form['disk']
        nodeType = request.form['nodeType']
        
        if nodeType == 'data':
            historical, middleManager = sizeData(cpu, mem, disk)
            type1 = 'Historical'
            sum1 = yaml.dump(historical, default_flow_style=False).replace(': ', '=')
            type2 = 'Middle Manager'
            sum2 = yaml.dump(middleManager, default_flow_style=False).replace(': ', '=')
            
        elif nodeType == 'query':
            broker, router = sizeQuery(cpu, mem)
            type1 = 'Broker'
            sum1 = yaml.dump(broker, default_flow_style=False).replace(': ', '=')
            type2 = 'Router'
            sum2 = yaml.dump(router, default_flow_style=False).replace(': ', '=')

        else:
            coordinator, overlord = sizeMaster(cpu, mem)
            type1 = 'Coordinator'
            sum1 = yaml.dump(coordinator, default_flow_style=False).replace(': ', '=')
            type2 = 'Overlord'
            sum2 = yaml.dump(overlord, default_flow_style=False).replace(': ', '=')

        return render_template('result.html', sum1=sum1, sum2=sum2, 
                                    type1=type1, type2=type2)


def sizeMaster(cpu, mem):
    coordinator = {}
    XmC = int((int(mem)* 1024) * 0.5)
    coordinator['jvm.config.Xms'] = '-Xms' + str(XmC) + 'm'
    coordinator['jvm.config.Xmx'] = '-Xmx' + str(XmC) + 'm'

    overlord = {}
    XmO = int((int(mem)* 1024) * 0.25)
    overlord['jvm.config.Xms'] = '-Xms' + str(XmO) + 'm'
    overlord['jvm.config.Xmx'] = '-Xmx' + str(XmO) + 'm'
    return coordinator, overlord


def sizeQuery(cpu, mem):
    broker = {}
    broker['druid.processing.numThreads'] = 1
    broker['druid.broker.http.numConnections'] = 20
    broker['druid.processing.buffer.sizeBytes'] = 500000000
    broker['druid.processing.numMergeBuffers'] = roundUpToEven(int(mem) * 0.8)
    broker['druid.server.http.numThreads'] = max(roundUpToEven(int(cpu) * 3.5), 20)
    XmB = int(int(mem) * 0.35)
    broker['jvm.config.Xms'] = '-Xms' + str(XmB) + 'g'
    broker['jvm.config.Xmx'] = '-Xmx' + str(XmB) + 'g'
    maxDirect = int(int(mem) * 0.56)
    broker['jvm.config.xmx.MaxDirectMemorySize'] = '-XX:MaxDirectMemorySize=' + str(maxDirect) + 'g'
    
    router = {}
    XmR = roundUpDiv(int(mem), 16)
    router['jvm.config.Xms'] = '-Xms' + str(XmR) + 'g'
    router['jvm.config.Xmx'] = '-Xmx' + str(XmR) + 'g'
    router['druid.router.http.numConnections'] = 20
    router['druid.server.http.numThreads'] = max(roundUpToEven(int(cpu) * 3), 15)
    router['druid.router.http.numMaxThreads'] = router['druid.server.http.numThreads']
    return broker, router


def sizeData(cpu, mem, disk):
    historical = {}
    historical['druid.processing.numThreads'] = int(cpu) - 1
    historical['druid.server.http.numThreads'] = historical['druid.processing.numThreads'] * 3
    historical['druid.processing.numMergeBuffers'] = roundUpDiv(historical['druid.processing.numThreads'], 4)
    if int(mem) > 64:
        historical['druid.processing.buffer.sizeBytes'] = 700000000
    else:
        historical['druid.processing.buffer.sizeBytes'] = 500000000
    historical['druid.server.maxSize'] = int(disk) * 1000000000
    historical['druid.cache.sizeInBytes'] = int(mem) * 10000000
    historical['druid.segmentCache.locations'] = "[{\"path\":\"/mnt/var/druid/segment-cache\",\"maxSize\":" + str(historical['druid.server.maxSize']) + "}]"
    XmH = min(int(0.5 * int(cpu)), 24)
    historical['jvm.config.Xms'] = '-Xms' + str(XmH) + 'g'
    historical['jvm.config.Xmx'] = '-Xmx' + str(XmH) + 'g'
    maxDirect = int(((historical['druid.processing.numThreads'] + 
                      historical['druid.processing.numMergeBuffers'] +1 ) *
                      historical['druid.processing.buffer.sizeBytes'] ) / 100000000)
    historical['jvm.config.xmx.MaxDirectMemorySize'] = '-XX:MaxDirectMemorySize=' + str(maxDirect) + 'g'

    middleManager = {}
    if int(mem) >= 256:
        XmMM = 512
    elif int(mem) >= 128:
        XmMM = 256
    else:
        XmMM = 128
    middleManager['jvm.config.Xms'] = '-Xms' + str(XmMM) +'g'
    middleManager['jvm.config.Xmx'] = '-Xmx' + str(XmMM) +'g'
    middleManager['druid.worker.capacity'] = roundUpDiv(int(cpu),2.67)
    middleManager['druid.indexer.fork.property.druid.processing.buffer.sizeBytes'] = 300000000
    middleManager['druid.indexer.fork.property.druid.processing.numMergeBuffers'] = 2
    middleManager['druid.indexer.fork.property.druid.processing.numThreads'] = 2
    middleManager['druid.indexer.fork.property.druid.server.http.numThreads'] = 50
    middleManager['druid.indexer.runner.javaOpts'] = '-server -Xmx3g -XX:+IgnoreUnrecognizedVMOptions -XX:MaxDirectMemorySize=10g -Duser.timezone=UTC -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/mnt/tmp/druid-peon.hprof -Dfile.encoding=UTF-8 -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager'
    return historical, middleManager


if __name__ == "__main__":
    app.run(host='0.0.0.0')