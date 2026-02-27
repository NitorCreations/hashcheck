(ns hashcheck.core
  (:gen-class)
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer [deftest is]])
  (:import
   [java.io File FileInputStream]
   [java.security MessageDigest]))

(defn transduce-leaves
  ([root children reducer]
   (transduce-leaves root children reducer (reducer) identity))

  ([root children reducer initial-value]
   (transduce-leaves root children reducer initial-value identity))

  ([root children reducer initial-value transducer]
   (let [reducing-function (transducer reducer)]
     (loop [value initial-value
            inner-nodes [root]]
       (if-some [inner-node (first inner-nodes)]
         (let [[value inner-nodes] (loop [value value
                                          the-children (children inner-node)
                                          inner-nodes (rest inner-nodes)]
                                     (if-some [child (first the-children)]
                                       (if (children child)
                                         (recur value
                                                (rest the-children)
                                                (conj inner-nodes child))
                                         (let [result (reducing-function value
                                                                         child)]
                                           (if (reduced? result)
                                             (reducing-function @result)
                                             (recur result
                                                    (rest the-children)
                                                    inner-nodes))))
                                       [value inner-nodes]))]
           (recur value
                  inner-nodes))
         (reducing-function value))))))

(deftest test-transduce-leaves
  (is (= [:b :g :e]
         (transduce-leaves {:a :b
                            :c {:d :e}
                            :f :g}
                           (fn [value]
                             (if (map? value)
                               (vals value)
                               nil))
                           conj))))

(defn transduce-files! [directory-path transducer]
  (transduce-leaves (File. directory-path)
                    (fn [file]
                      (when (.isDirectory file)
                        (.listFiles file)))
                    (constantly nil)
                    nil
                    transducer))
(defn take-until-stopped [stopped?-atom]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (if @stopped?-atom
         (ensure-reduced result)
         (rf result input))))))

(defn md5 [file-path]
  (let [digest (MessageDigest/getInstance "MD5" #_"SHA-256")]
    (with-open [fis (FileInputStream. (File. file-path))]
      (let [buffer (byte-array 8192)]
        (loop []
          (let [len (.read fis buffer)]
            (when (pos? len)
              (.update digest buffer 0 len)
              (recur))))))
    (let [hash-bytes (.digest digest)]
      (apply str
             (map #(format "%02x" (bit-and % 255))
                  hash-bytes)))))

(defn write-file-hashes [directory-path output-file-path]
  (let [stopped?-atom (atom false)
        already-hashed-files-set (if (.exists (File. output-file-path))
                                   (with-open [reader (io/reader output-file-path)]
                                     (->> (line-seq reader)
                                          (map edn/read-string)
                                          (map :path)
                                          (into #{})))
                                   #{})
        directory-path-length (count (.getAbsolutePath (File. directory-path)))
        written-count-atom (atom 0)
        processed-count-atom (atom 0)]
    (with-open [writer (io/writer output-file-path :append true)]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. (fn []
                                   (reset! stopped?-atom true)
                                   (println "sutting down")
                                   (Thread/sleep 500))))
      (transduce-files! directory-path
                        (comp (take-until-stopped stopped?-atom)
                              (map (fn [file]
                                     (when (and (not (= 0 @processed-count-atom))
                                                (= 0 (mod @processed-count-atom 500)))
                                       (println @processed-count-atom
                                                "files have been processed out of which"
                                                (- @processed-count-atom
                                                   @written-count-atom)
                                                "were already in the target file."))
                                     (swap! processed-count-atom inc)
                                     (subs (.getAbsolutePath file)
                                           (inc directory-path-length))))
                              (remove already-hashed-files-set)
                              (map (fn [path]
                                     (.write writer
                                             (str (pr-str {:path path
                                                           :md5 (md5 (str directory-path "/" path))})
                                                  "\n"))
                                     (.flush writer)
                                     (swap! written-count-atom inc))))))
    (if @stopped?-atom
      (println "Got interrupted.")
      (println "All files were processed."))
    (println "Wrote" @written-count-atom "files.")
    (when (not (= @written-count-atom
                  @processed-count-atom))
      (println (- @processed-count-atom
                  @written-count-atom) "hashes were already in the target file."))))

(comment
  (write-file-hashes "temp/hashtest" "temp/file-hashes")
  ) ;; TODO: remove me

(def commands [#'write-file-hashes])

(defn -main [& command-line-arguments]
  (let [[command-name & arguments] command-line-arguments]
    (if-let [command (first (filter (fn [command]
                                      (= command-name
                                         (name (:name (meta command)))))
                                    commands))]
      (do (try (apply command arguments)
               (catch Exception exception
                 (println exception)))
          (.flush *out*)
          (shutdown-agents) ;; see https://clojureverse.org/t/why-doesnt-my-program-exit/3754/2
          )

      (do (println "Usage:")
          (println "------------------------")
          (println (->> commands
                        (map (fn [command-var]
                               (str (:name (meta command-var))
                                    ": "
                                    (:arglists (meta command-var))
                                    "\n"
                                    (:doc (meta command-var)))))
                        (string/join "------------------------\n")))))))
