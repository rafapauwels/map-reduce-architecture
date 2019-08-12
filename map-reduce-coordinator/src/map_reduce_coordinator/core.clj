(ns map-reduce-coordinator.core
  (:gen-class))
(import '[java.net DatagramSocket
          DatagramPacket])

(defn split-and-propagate
  [urls mappers]
  (let [n-mappers (count mappers)]
    (partition-all n-mappers urls) ;; esta separando em grupos de 3 e não em 3 grupos
    ))

(defn receive-urls!
  [^DatagramSocket socket]
  (let [buffer (byte-array 512)
        package (DatagramPacket. buffer 512)]
    (.receive socket package)
    (clojure.edn/read-string (String. (.getData package)
                                      0 (.getLength package)))))

(defn loop-receive-urls!
  [f socket mappers]
  (future (while true (f (receive-urls! socket) mappers))))

(defn -main
  [& args]
  (let [socket (DatagramSocket. 9500)
        mappers '({:url "localhost" :port 9501}
                  {:url "localhost" :port 9502}
                  {:url "localhost" :port 9503})]
    (loop-receive-urls! split-and-propagate
                        socket
                        mappers)))
