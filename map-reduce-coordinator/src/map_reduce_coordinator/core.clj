(ns map-reduce-coordinator.core
  (:gen-class))
(import '[java.net DatagramSocket
          DatagramPacket
          InetSocketAddress])

(defn split-in
  [n mapa]
  (->> mapa
       (map-indexed (fn [i x] [(mod i n) x]))
       (group-by first)
       vals
       (mapv #(mapv peek %))))

(defn send-to-mapper!
  [data mapper]
  (let [payload (.getBytes (pr-str data))
        payload-size (min (alength payload) 512)
        target (InetSocketAddress. (:url mapper) (:port mapper))
        package (DatagramPacket. payload payload-size target)]
    package))

(defn propagate!
  [splitted-urls mappers socket]
  (map #(.send socket %) (map send-to-mapper! splitted-urls mappers)))

(defn split-and-propagate
  [urls mappers socket]
  (let [n-mappers (count mappers)]
    (propagate (split-in n-mappers urls) mappers socket)))

(defn receive-urls!
  [^DatagramSocket socket]
  (let [buffer (byte-array 512)
        package (DatagramPacket. buffer 512)]
    (.receive socket package)
    (clojure.edn/read-string (String. (.getData package)
                                      0 (.getLength package)))))
(defn loop-receive-urls!
  [f socket mappers]
  (future (while true (f (receive-urls! socket) mappers socket))))

(defn -main
  [& args]
  (let [socket (DatagramSocket. 9500)
        mappers '({:url "localhost" :port 9600}
                  {:url "localhost" :port 9502}
                  {:url "localhost" :port 9503})]
    (loop-receive-urls! split-and-propagate
                        socket
                        mappers)))
