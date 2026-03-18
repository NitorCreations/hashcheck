(defproject hashcheck "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [medley "1.4.0"]]
  :main hashcheck.core
  :uberjar-name "hashcheck.jar"
  :profiles {:uberjar {:aot [hashcheck.core]}})
