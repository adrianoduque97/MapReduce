import threading
import operator
import re
from multiprocessing.pool import ThreadPool
import time

listTexts = []
listaNodo = []
reducerNodos = {}

combbool = False
combbool2 = True

redbool= False
redbool2 =True


def mapper(mensaje, i):
    h = open("Map"+str(i)+".txt", 'w')
    contador = 0
    for line in mensaje:
        palabras = line.split()

        for word in palabras:
            listTemp = []
            listTemp.append(word)
            listTemp.append(1)
            listaNodo.append(listTemp)
            h.write(str(listTemp[0])+" "+str(listTemp[1])+"\n")
        contador = contador + len(palabras)
    h.close()
    print("Ejecutando: " + str(threading.current_thread()) + "\nPalabras disponibles: " + str((contador)))


def reducer(listaMensaje, i):
    reducerTemp = {}
    h=open("Reducer"+str(i)+".txt",'w')
    for nodo in listaMensaje:
        count = nodo[1]

        try:
            reducerTemp[nodo[0]] = reducerTemp[nodo[0]] + count
        except:
            reducerTemp[nodo[0]] = count

    for key, value in reducerTemp.items():
        h.write(str(key)+" "+str(int(value))+"\n")
    h.close()
    return reducerTemp


def shuffler(listaReducida):
    h=open("Sorted.txt",'w')

    listaShuffler = sorted(listaReducida.items(), key=operator.itemgetter(1))
    listaShuffler.reverse()
    for elemento in listaShuffler:
        h.write(str(elemento[0])+" "+str(elemento[1])+"\n")

    print("Lista despues de Sorter Final" + str(listaShuffler))


def sorter(red1, red2):
    listaTemporal = red1
    for el in red2:
        listaTemporal.append(el)

    listaTemporal = sorted(listaTemporal)

    return listaTemporal


def coordinator(texto):
    try:
        i = 0
        for lin in texto:
            if i % 25 == 0 or i == 0:
                file = open(str(i) + ".txt", "w")
                listTexts.append(str(i) + ".txt")
            file.write(lin)
            i = i + 1
        file.close()
        print("Ficheros generados:\n"+str(listTexts))

        print("\n[MAPPER INICIADO]")
        i = 1
        nodosRevivir =[]
        for texto in listTexts:
            g = open(texto, 'r')
            men = g.readlines()
            t = threading.Thread(target=mapper, args=(men,i), name=str(texto))
            print("\nIniciando thread " + t.getName())
            print("Palabras texto: " + str(men))

            if i % 3 != 0:
                t.start()
                t.join()
            else:
                print("Mapper: " + t.getName() + "  no inicia correctamente ")
                nodosRevivir.append(t)

            i = i + 1
        if len(nodosRevivir) > 0:
            for nodo in nodosRevivir:
                revivirNodo(nodo)
        else:
            print("\nMAPPER EXITOSO\n")
        time.sleep(2)
    except:
        print("Nodo coordinador fallo, reintente la tarea")


   # if ()
    dictlist=[]
    for i in range(1,8):
        with open("Map"+str(i)+".txt") as f:
            for lin in f:
                (key, val) = lin.split()
                tempo =[key,int(val)]
                dictlist.append(tempo)

    combinaterReduced(dictlist[0:len(dictlist)//2], dictlist[len(dictlist)//2:])

    time.sleep(2)

    redList=[]
    for i in range(1,3):
        with open("Reducer"+str(i)+".txt") as f:
            for lin in f:
                (key, val) = lin.split()
                tempo =[key,int(val)]
                redList.append(tempo)

    print("\n[SHUFFLER INICIADO]\n")
    listSorted = sorter(redList[0:len(redList)//2], redList[len(redList)//2:])
    print(str(listSorted)+"\n")

    reducerFinal(listSorted)

    print("\n[FINAL STEP SORTER]\n")

    shuffler(reducerNodos)


def revivirNodo(t):
    t.start()
    t.join()
    print("\nNODO REVIVIDO:" + str(t.name) +"\n")

def combinaterReduced(l,j):
    print("\n[COMBINER INICIADO]")

    revivirNodos =[]
    reducer1=0
    reducer2=0
    pool = ThreadPool(processes=2)

    if combbool is True:
        reducer1 = pool.apply_async(reducer,(l,1))
    else:
        print("Combiner1 NO inicia correctamente")
        revivirNodos.append(l)
    if combbool2 is True:
        reducer2 = pool.apply_async(reducer, (j,2))
    else:
        print("Combiner2 NO inicia correctamente")
        revivirNodos.append(j)


    if len(revivirNodos) >0:
        try:
            if reducer1 is 0:
                reducer1 = pool.apply_async(reducer, (revivirNodos[0], 1))

            if reducer2 is 0:
                reducer2 = pool.apply_async(reducer, (revivirNodos[1], 2))
            print("NODOS REVIVIDOS")
        except:
            print("Corrigiendo")

    return_val = reducer1.get()
    return_val2 = reducer2.get()
    print("\nCombinator 1: \n" + str(return_val))
    print("\nCombinator  2: \n " + str(return_val2))

def reducerFinal(l):
    print("\n[REDUCER INICIADO]")

    revivirNodos =[]
    reducer1=0
    reducer2=0
    pool = ThreadPool(processes=2)

    if redbool is True:
        reducer1 = pool.apply_async(reducer,((l[0:len(l) // 2],3)))  # tuple of args for foo
    else:
        print("Reducer1 NO inicia correctamente")
        revivirNodos.append(l[0:len(l) // 2])
    if redbool2 is True:
        reducer2 = pool.apply_async(reducer, (l[len(l) // 2:len(l)],4))
    else:
        print("Reducer2 NO inicia correctamente")
        revivirNodos.append(l[len(l) // 2:len(l)])

    if len(revivirNodos) >0:
        try:
            if reducer1 is 0:
                reducer1 = pool.apply_async(reducer, (revivirNodos[0], 3))

            if reducer2 is 0:
                reducer2 = pool.apply_async(reducer, (revivirNodos[1], 4))
            print("NODOS REVIVIDOS")
        except:
            print("Corrigiendo")
    time.sleep(2)

    return_val = reducer1.get()
    return_val2 = reducer2.get()
    print("\n(Reducer 1: \n" + str(return_val))
    print("\nReducer 2: \n " + str(return_val2))

    temporal = combinarDict(return_val, return_val2)
    reducerNodos.update(temporal)


def combinarDict(d1, d2):  # Metodo para combinar los dos diccionarios de salida
    resultado = dict(d1)

    for key in d2.keys():
        if key in d1.keys():
            resultado[key] = (d1.get(key) + d2.get(key))
        else:
            resultado[key] = d2.get(key)

    return resultado


if __name__ == "__main__":
    textPrincipal = open("prueba4.txt", 'r')
    mensaje = textPrincipal.readlines()
    tempMens = []

    for line in mensaje:
        tempMens.append(re.sub(';|,|:|\'|"|\)|\(', "", line.lower()))

    try:
        coordinator(tempMens)
    except:
        print("NODO COORDINADOR FALLA")

    print("[FIN EJECUCION]")

    h = open("final.txt", 'w')
    h.write(str(reducerNodos))
    h.close()





