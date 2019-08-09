(ns map-reduce-client.core
  (:require [clojure.string :as str])
  (:gen-class))

(defn mapeia-urls
  "Carrega lista de URLs do FS separado por \n e retorna um mapa no formato 
  {:url url}, (keyword (re-find #\"(?<=www\\.).*(?=\\.com|\\.edu)\" %))\""
  [arquivo]
  (map 
   #(hash-map 
     :url
     %)
   (str/split-lines (slurp arquivo))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
