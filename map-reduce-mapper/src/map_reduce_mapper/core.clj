(ns map-reduce-mapper.core
  (:gen-class))
(import '[java.net DatagramSocket
          DatagramPacket
          InetSocketAddress])

(defn extract-url!
  [url]
  (slurp url))

(defn extract-urls!
  [urls]
  (map extract-url urls))

(defn receive-urls!
  [^DatagramSocket socket]
  (let [buffer (byte-array 512)
        package (DatagramPacket. buffer 512)]
    (.receive socket package)
    (clojure.edn/read-string (String. (.getData package)
                                      0 (.getLength package)))))
(defn loop-receive-urls!
  [f socket]
  (future (while true (f (receive-urls! socket) socket))))

(defn -main
  [& args]
  (let [socket (DatagramSocket. 9600)]
    (loop-receive-urls! extract-urls
                        socket)))
