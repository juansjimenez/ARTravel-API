from flask import Flask, render_template, request, abort, json
from pymongo import MongoClient
from werkzeug.exceptions import BadRequest
import pymongo
import os


USER_KEYS = ['name', 'age', 'description']
MESSAGE_KEYS = ['message', 'sender', 'receptant', 'lat', 'long', 'date']
SEARCH_KEYS = ["desired", "required", "forbidden", "userId"]

USER = "grupo90"
PASS = "grupo90"
DATABASE = "grupo90"

# MONGO_URL = os.environ.get('MONGO_URL')
# if not MONGO_URL:
#     MONGO_URL = 'localhost'

MONGO_URL = 'mongodb+srv://admin:rasputin00@appscluster.bvybr.mongodb.net/ARTravel?retryWrites=true&w=majority'

client = MongoClient(MONGO_URL)


usuarios = db.users
mensajes = db.messages

'''
Usuarios:
  "uid": <id del usuario>,
  "name": <nombre>,
  "age": <edad>,
  "description": <descripcion del usuario>
'''
'''
Mensajes:
  "mid": <id del mensaje>,
  "message": <contenido del mensaje>,
  "sender": <id de quien envia el mensaje>,
  "receptant": <id de quien recibe el mensaje>,
  "lat": <latitud de donde se envia el mensaje>,
  "long": <longitud desde donde se envia el mensaje>,
  "date": <fecha en la cual se envia el mensaje>
'''

app = Flask(__name__)

app.config['MONGO_URI'] = MONGO_URL

@app.route("/")
def home():
    '''
    Página de inicio
    '''
    return "<h1>ARTravel Users and Messages API</h1>"

@app.route("/users")
def get_users():
    '''
    Obtiene todos los usuarios
    '''
    # Omitir el _id porque no es json serializable
    resultados = list(usuarios.find({}, {"_id": 0}))
    return json.jsonify(resultados)

@app.route("/users/<int:uid>")
def get_user(uid):
    '''
    Obtiene el usuario de id entregada y sus mensajes enviados
    '''
    users = list(usuarios.find({"uid": uid}, {"_id": 0}))
    if len(users) > 0:
        messages = list(mensajes.find({"sender": uid}, {"_id": 0}))
        return json.jsonify(user_info = users, user_messages = messages)
    else:
        return json.jsonify({"error": f"El usuario con id {uid} no existe"})

@app.route("/messages")
def get_messages():
    '''
    Obtiene todos los mensajes
    '''
    uid1 = request.args.get('id1', False, int)
    uid2 = request.args.get('id2', False, int)
    if uid1 and uid2:
        resultados = list(mensajes.find({"$or": [{"sender": uid1, "receptant": uid2}, {"sender": uid2, "receptant": uid1}]}, {"_id": 0}))
        if len(resultados) > 0:
            return json.jsonify(resultados)
        else:
            return json.jsonify({"error": f'No hay mensajes entre los usuarios con id {uid1} y {uid2}.'})
    else:
        resultados = list(mensajes.find({}, {"_id": 0}))
        return json.jsonify(resultados)

@app.route("/messages/<int:mid>")
def get_message(mid):
    '''
    Obtiene el mensaje asociado al id entregado
    '''
    messages = list(mensajes.find({"mid": mid}, {"_id": 0}))
    if len(messages) > 0:
        return json.jsonify(messages)
    else:
        return json.jsonify({"error": f'El mensaje con id {mid} no existe.'})

@app.route("/messages", methods=['POST'])
def create_message():
    '''
    Crea un nuevo mensaje en la base de datos
    Se  necesitan todos los atributos de model, a excepcion de _id
    '''
    if (request.json):
        datos_recibidos = request.json.keys()
        if set(datos_recibidos) == set(MESSAGE_KEYS):
            data = {key: request.json[key] for key in MESSAGE_KEYS}
            #Revisar si los atributos son validos
            #sender
            sender = data["sender"]
            revisar_sender = list(usuarios.find({"uid": sender}, {"_id": 0}))
            if len(revisar_sender) > 0:
                #receptant
                receptant = data["receptant"]
                revisar_receptant = list(usuarios.find({"uid": receptant}, {"_id": 0}))
                if len(revisar_receptant) > 0:
                    if isinstance(data["lat"], float) and isinstance(data["long"], float) and isinstance(data["message"], str) and isinstance(data["date"], str):
                        count = usuarios.count_documents({})
                        data['mid'] = count + 1
                        mensajes.insert_one(data)
                        return json.jsonify({'success': True, 'message': 'El mensaje fue creado exitosamente.'})
                    else:
                        return json.jsonify({'success': False, 'message': 'Alguno de los datos ingresados no tiene el formato correcto.'})
                else:
                    return json.jsonify({'success': False, 'message': 'El mensaje al que se le desea enviar un mensaje no existe.'})
            else:
                return json.jsonify({'success': False, 'message': 'El mensaje que desea enviar un mensaje no existe.'})
        else:
            return json.jsonify({'success': False, 'message': f'No se ha podido crear el mensaje dado que faltaron los siguientes datos: {", ".join(set(MESSAGE_KEYS) - set(datos_recibidos))}'})
    else:
        return json.jsonify({'success': False, 'message': f'No se ha podido crear el mensaje dado que faltaron los siguientes datos: {", ".join(MESSAGE_KEYS)}'})

@app.route("/message/<int:mid>", methods=['DELETE'])
def delete_message(mid):
    '''
        Elimina el mensaje asociado al id entregado
    '''
    messages = list(mensajes.find({"mid": mid}, {"_id": 0}))
    if len(messages) > 0:
        mensajes.delete_one({"mid": mid})
        return json.jsonify({'success': True, "message": f'El mensaje con id {mid} fue eliminado exitosamente.'})
    else:
        return json.jsonify({'success': False, "message": f'El mensaje con id {mid} no existe, por lo que no pudo ser eliminado.'})

@app.route("/text-search")
def busqueda_texto():
    data = dict()
    f_admitted = 0
    f_forb = False
    id = False
    for key in SEARCH_KEYS:
        try:
            if isinstance(request.json[key], list):
                if len(request.json[key]) != 0:
                    data[key] = request.json[key]
                    if key == "required" or key == "desired":
                        f_admitted += 1
                    elif key == "forbidden":
                        f_forb = True
                else:
                    data[key] = None
            elif isinstance(request.json[key], int):
                data[key] = request.json[key]
                id = True
            else:
                data[key] = None

        except (KeyError, TypeError, BadRequest):
            data[key] = None

    # desired, required and forbidden empty or non existent
    # id not int or non existent
    if f_admitted == 0 and not f_forb and not id:
        resultados = list(mensajes.find({}, {"_id": 0}))
        return json.jsonify(resultados)

    # desired, required and forbidden empty or non existent
    # id exists
    if f_admitted == 0 and not f_forb and id:
        resultados = list(mensajes.find({"sender": data["userId"]}, {"_id": 0}))
        return json.jsonify(resultados)

    # only forbidden exists or is the only one not empty
    # id not int or non existent
    if f_admitted == 0 and f_forb and not id:
        forb = " ".join(set(data["forbidden"]))
        all = list(mensajes.find({}, {"_id": 0}))
        to_delete = list(mensajes.find({"$text": {"$search": f"{forb}"}}, {"_id": 0}))
        resultados = [msg for msg in all if msg not in to_delete]
        return json.jsonify(resultados)

    # only forbidden exists or is the only one not empty
    # exists
    if f_admitted == 0 and f_forb and id:
        forb = " ".join(set(data["forbidden"]))
        all = list(mensajes.find({"sender": data["userId"]}, {"_id": 0}))
        to_delete = list(mensajes.find({"sender": data["userId"],
                                        "$text": {"$search": f"{forb}"}}, {"_id": 0}))
        resultados = [msg for msg in all if msg not in to_delete]
        return json.jsonify(resultados)

    # e.o.c
    desired = ""
    required = ""
    forbidden = ""
    if data["desired"]:
        desired = " ".join(set(data["desired"]))

    if data["required"]:
        for frase in set(data["required"]):
            required += f"\"{frase}\""

    if data["forbidden"]:
        forbidden = "-" + " -".join(set(data["forbidden"]))

    search_string = desired + " " + required + " " + forbidden

    if id:
        resultados = list(mensajes.find({"sender": data["userId"], "$text": {"$search": f"{search_string}"}},
                          {"_id": 0, "score": {"$meta": "textScore"}}).sort([('score', {'$meta': 'textScore'})]))
    else:
        resultados = list(mensajes.find({"$text": {"$search": f"{search_string}"}},
                          {"_id": 0, "score": {"$meta": "textScore"}}).sort([('score', {'$meta': 'textScore'})]))

    return json.jsonify(resultados)


if __name__ == "__main__":
    # app.run()
    app.run(debug=True)
