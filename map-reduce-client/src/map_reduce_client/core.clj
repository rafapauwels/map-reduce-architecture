(ns map-reduce-client.core
  (:require [clojure.string :as str])
  (:gen-class))
(import '[java.net DatagramSocket
          DatagramPacket
          InetSocketAddress])

(defn load-urls!
  "Carrega lista de URLs do FS separado por \n e retorna um mapa no formato"
  [arquivo]
  (map 
   #(hash-map 
     :url
     %)
   (str/split-lines (slurp arquivo))))

(defn send-to-coordinator
  [^DatagramSocket socket urls coordinator-ip]
  (let [payload (.getBytes (pr-str urls))
        payload-size (min (alength payload) 512)
        target (InetSocketAddress. coordinator-ip 9500)
        package (DatagramPacket. payload payload-size target)]
    (.send socket package)))

(defn -main
  [& args]
  (let [socket (DatagramSocket. 9400)
        urls (load-urls! (first *command-line-args*))]
    (send-to-coordinator socket urls "localhost")))
