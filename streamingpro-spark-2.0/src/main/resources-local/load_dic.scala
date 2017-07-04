val broadcastVar = context.sparkSession.sparkContext.broadcast(Array(1, 2, 3))
broadcastVar.id