(*
  Asynchronous access to MySQL relying on a pool of processes,
  each in charge of one database connection.
*)

open Log
open Printf
open Lwt

(*
   Optional logging of debug information.
*)
let debug_logger = ref ignore

let db_settings () =
  let conf = Config.get () in
  {
    Mysql.dbhost = Some conf.Config_t.db_host;
    dbname = Some conf.Config_t.db_name;
    dbport = None;
    dbpwd = conf.Config_t.db_password;
    dbuser = Some "root";
    dbsocket = None;
  }

(* called only in a worker *)
let connect () =
  Mysql.connect (db_settings ())

let get_connection dbd_ref =
  match !dbd_ref with
    | None ->
        let dbd = connect () in
        dbd_ref := Some dbd;
        dbd
    | Some dbd ->
        try Mysql.ping dbd; dbd
        with Mysql.Error msg ->
          let dbd = connect () in
          dbd_ref := Some dbd;
          dbd

type worker_env = {
  dbd_ref : Mysql.dbd option ref;
}

(* Thread-local storage, keyed by thread ID *)
let tls = Hashtbl.create 100

let make_protector () =
  let mutex = Mutex.create () in
  let protect f =
    try
      Mutex.lock mutex;
      let result = f () in
      Mutex.unlock mutex;
      result
    with e ->
      Mutex.unlock mutex;
      raise e
  in
  protect

let init_worker () : worker_env = {
  dbd_ref = ref None;
}

let getenv =
  let protect = make_protector () in
  let get () =
    let k = Thread.self () in
    try Hashtbl.find tls k
    with Not_found ->
      let v = init_worker () in
      protect (fun () ->
        Hashtbl.add tls k v
      );
      v
  in
  get

(*
  Call a function that should run in one of the threads of the pool.
  The maximum number of threads could be configured with Lwt_preemptive.init.
*)
let call user_f =
  let f () =
    let worker_env = getenv () in
    user_f worker_env
  in
  Cloudwatch.time "mysql.any.latency" (fun () ->
    Lwt_preemptive.detach f ()
  )

(*****)

type exec_result =
    Result of (Mysql.result * int64(*rows affected*) )
  | Error of string

let unwrap_result (x : exec_result) : Mysql.result * int64 =
  match x with
      Result y -> y
    | Error s -> failwith s

let mysql_exec statement handler =
  !debug_logger statement;
  call (fun env ->
    let exec_result =
      try
        let dbd = get_connection env.dbd_ref in
        let result = Mysql.exec dbd statement in
        match Mysql.errmsg dbd with
            None -> Result (result, Mysql.affected dbd)
          | Some s -> Error ("mysql error " ^ s)
      with e ->
        match e with
            Mysql.Error s -> Error ("mysql error " ^ s)
          | e ->
              Error
                ("exception raised by Mysql.exec: " ^ (string_of_exn e))
    in
    (* Exceptions raised by handler are caught and logged in the worker
       because they can't be marshalled. *)
    handler exec_result
  )

let test () =
  Lwt_main.run (
    mysql_exec "create database if not exists mytest;" ignore >>= fun () ->
    return true
  )

let tests = [ "basic nproc test", test ]
