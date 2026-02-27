(defproject hashcheck "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.12.4"]]
  :main hashcheck.core
  :profiles {:uberjar {:aot [hashcheck.core]}})
