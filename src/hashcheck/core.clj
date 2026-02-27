(ns hashcheck.core
  (:gen-class)
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer [deftest is]])
  (:import
   [java.io File FileInputStream]
   [java.security MessageDigest]))

;; (defn get-all-files [dir]
;;   (let [d (File. dir)]
;;     (if (.isDirectory d)
;;       (mapcat (fn [f]
;;                 (if (.isDirectory f)
;;                   (get-all-files (.getPath f))
;;                   [f]))
;;               (.listFiles d))
;;       [])))

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

(defn transduce-files
  ([directory-path reducer]
   (transduce-files directory-path reducer (reducer) identity))

  ([directory-path reducer initial-value]
   (transduce-files directory-path reducer initial-value identity))

  ([directory-path reducer initial-value transducer]
   (transduce-leaves (File. directory-path)
                     (fn [file]
                       (when (.isDirectory file)
                         (.listFiles file)))
                     reducer
                     initial-value
                     transducer)))

(defn process-files!
  ([directory-path process-file!]
   (process-files! directory-path process-file! identity))

  ([directory-path process-file! transducer]
   (transduce-files directory-path
                    (completing (fn [_value file]
                                  (process-file! file)))
                    nil
                    transducer)))

(defn sha256-hash [file-path]
  (let [digest (MessageDigest/getInstance "SHA-256")]
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

;; (defn hash-files [files]
;;   (map (fn [file]
;;          {:file file
;;           :hash (sha256-hash file)})
;;        files))

;; (comment
;;   (transduce-files "temp/hashtest" conj)

;;   (->> (get-all-files "/Users/jukka/nitor-src/hashcheck/temp/hashtest")
;;        (map File/.getAbsolutePath)
;;        hash-files)
;;   (.getAbsolutePath ^java.io.File (File. "temp/hashtest"))


;;   )

;; (defn list-files [directory-path list-file-path]
;;   (
;;    (let [already-listed-files-set (if (.exists (File. list-file-path))
;;                                     (with-open [reader (io/reader list-file-path)]
;;                                       (into #{} (line-seq reader)))
;;                                     #{})]
;;      (transduce-files directory-path (fn [file]
;;                                        )(constantly nil) nil ))))

(defn take-while-atom [take-more?-atom]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (if @take-more?-atom
         (rf result input)
         (ensure-reduced result))))))

(defn write-file-paths [directory-path output-file-path]
  (let [running?-atom (atom true)
        already-listed-files-set (if (.exists (File. output-file-path))
                                   (with-open [reader (io/reader output-file-path)]
                                     (into #{} (line-seq reader)))
                                   #{})
        directory-path-length (count (.getAbsolutePath (File. directory-path)))
        count-atom (atom 0)]

    (with-open [writer (io/writer output-file-path :append true)]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. (fn []
                                   (reset! running?-atom false)
                                   (println "sutting down")
                                   (Thread/sleep 500))))
      (process-files! directory-path
                      (fn [file-path]
                        (.write writer
                                (str file-path
                                     "\n"))
                        (.flush writer)
                        (swap! count-atom inc)
                        (when (= 0 (mod @count-atom 50000))
                          (println @count-atom "paths have been written")))
                      (comp (map (fn [file]
                                   (subs (.getAbsolutePath file)
                                         (inc directory-path-length))))
                            (remove already-listed-files-set)
                            (take-while-atom running?-atom))))
    (if @running?-atom
      (println "All paths were written.")
      (println "Got interrupted after writing" @count-atom "paths"))))

(comment
  (write-file-paths "temp/hashtest" "temp/file-list")
  ) ;; TODO: remove me

(def commands [#'write-file-paths])

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


;; write clojure code that registeres a funciton to be called before the process is shut down
(defn add-shutdown-hook [f]
  (.addShutdownHook (Runtime/getRuntime) (Thread. f)))


;; (add-shutdown-hook #(println "Process shutting down..."))
