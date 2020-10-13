from py4j.java_gateway import JavaGateway, CallbackServerParameters

class PythonListener(object):

    def __init__(self, gateway):
        self.gateway = gateway

    def notify(self, obj):
        print("Notified by Java")
        print(obj)
        gateway.jvm.System.out.println("Hello from python!")
        gateway.jvm.System.out.println(obj)
        gateway.entry_point.perrito(obj)

        return "A Return Value"

    #Determina a que interfaz llava corresponderan los entrypoint de este objeto python
    class Java:
        implements = ["listener.ExampleListener"]


if __name__ == '__main__':
    # Java program gateway
    gateway = JavaGateway(callback_server_parameters=CallbackServerParameters())

    #Crea objeto Python invocable desde programa java
    listener = PythonListener(gateway)

    #Pasamos objeto python a programajava
    gateway.entry_point.registerListener(listener)
    #print(listener.notify("pepe"))

    # Connection refused
    #Esto es lo que necesitamos!!, ejecuta funcionalidad java con objetos input proveidos por python
    #gateway.entry_point.notifyAllListeners()
    #gateway.entry_point.perrito(4)
    #gateway.shutdown()