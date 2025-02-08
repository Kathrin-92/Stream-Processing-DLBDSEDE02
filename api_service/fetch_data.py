# https://www.umweltbundesamt.de/daten/luft/luftdaten/doc#get-/measures/json ---> alle measues; abrufbar nach stationsnummer
# Feinstaub, Kohlenmonoxid, Ozon, Schwefeldioxid,  Stichstoffdioxid
# stündlich --> es gibt aber insgesamt so 400 Stationen, d.h. man könnte die API Calls so bauen, dass es pro Sekunde die Daten einer
# Station abruft und dann hat man am Ende der Stunde einmal durchgelooped und fängt dann mit der nächsten Stunde an
# tbd wie regelmäßig man die API abfragen kann, habe dazu keine info gefunden
# oder nur Hamburg: würde besser zur Aufgabenstellung passen; hier gibt es 10 Messstationen (das wäre dann aber ein sehr slow stream)
# 10 Stationen, 5 Components = 50 Abfragen; ca. 1 Abfrage pro Minute könnte wiederum okay sein



# https://www.umweltbundesamt.de/daten/luft/luftdaten/doc#get-/components/json --> enthält info über die components, also feinstaub etc.
# https://www.umweltbundesamt.de/daten/luft/luftdaten/doc#get-/stations/json --> liste aller stationen mit nummern
# https://www.umweltbundesamt.de/daten/luft/luftdaten/doc#get-/stationsettings/json --> infos über station selbst, z.b. städtisch

# man könnte dann beispielhaft sowas nachbauen: https://www.umweltbundesamt.de/daten/luft/luftdaten/stationen/eJzrXpScv9BwUXEykEhJXGVkYGSqa2Cka2CyqCSTZ1Fe6oJFxSWLLUzMlqQkuhVBpQ11jSyA_JB8ZOXJiRPb8JoFMWdRWaLeotwqtkW5yU2LcxJLTjt4rpr3qlHu-OKcvPTTDirnXBw-WcwGAIDIOoA=



# alternativ: https://openweathermap.org/api
# das sind aber Wetterdaten, also z.B. temp, humidity, rain etc. nicht so sehr environment daten

# data generator: https://github.com/tinybirdco/mockingbird / https://github.com/MaterializeInc/datagen