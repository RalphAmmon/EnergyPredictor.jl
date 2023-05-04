using ModelWorker
conn = ModelWorker.connectDB()
ModelWorker.runFitter(conn)
ModelWorker.closeDB(conn)