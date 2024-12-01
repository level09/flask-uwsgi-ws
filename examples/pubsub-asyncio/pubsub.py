#!/usr/bin/env python3
from collections import deque
from flask import Flask, render_template, Blueprint
from flask_uwsgi_websocket import AsyncioWebSocket
import asyncio
import asyncio_redis

app = Flask(__name__)
wschat = Blueprint('wsBlueprint', __name__)
ws = AsyncioWebSocket(app)

@app.route('/')
def index():
    return render_template('index.html')

@wschat.route('/<string:channel>')
async def chat(ws, channel):
    await ws.send("Welcome to channel <{}>".format(channel))

    asyncio.get_event_loop().create_task(redis_subscribe(ws, channel))
    conn = await asyncio_redis.Connection.create()

    while True:
        msg = await ws.receive()
        if msg is not None:
            await conn.publish(channel, msg.decode('utf-8'))
        else:
            break

ws.register_blueprint(wschat, url_prefix='/websocket')

async def redis_subscribe(ws, channel):
    conn = await asyncio_redis.Connection.create()
    sub = await conn.start_subscribe()
    await sub.subscribe([channel])
    while ws.connected:
        reply = await sub.next_published()
        await ws.send(reply.value.encode('utf-8'))

if __name__ == '__main__':
    app.run(debug=True, asyncio=100, greenlet=True)
