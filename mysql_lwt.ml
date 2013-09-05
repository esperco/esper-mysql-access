(*
  Asynchronous access to MySQL relying on a pool of processes,
  each in charge of one database connection.
*)

open Printf
open Lwt

let db_settings () =
  let conf = Config.get () in
  {
    Mysql.dbhost = Some conf.Config_t.db_host;
    dbname = Some conf.Config_t.db_name;
    dbport = None;
    dbpwd = None;
    dbuser = Some "root";
    dbsocket = None;
  }

(* called only in a worker *)
let connect () =
  Mysql.connect (db_settings ())

type worker_env = {
  dbd : Mysql.dbd Lazy.t;
}

(*
  This is available in all workers without going through marshalling.
*)
let env : worker_env = {
  dbd = lazy (connect ());
}

(*
  Call a function that should run in the pool.

  The closure, its argument and the result will be marshalled, and therefore
  may not contain custom blocks (e.g. type Mysql.result won't pass).
*)
let current_pool = ref None

let get_current_pool () =
  match !current_pool with
      None ->
        let pool =
          Nproc.Full.create 10
            (fun () -> Lwt.return ())
            env
        in
        current_pool := Some pool;
        pool
    | Some x -> x

let pool () = fst (get_current_pool ())

let close_pool () =
  match !current_pool with
      None -> return ()
    | Some (x, _) ->
        current_pool := None;
        Nproc.Full.close x

let call user_f =
  let f service env () = user_f env in
  let p = pool () in
  Nproc.Full.submit p ~f ()

(*****)

type exec_result =
    Result of Mysql.result
  | Error of string

let unwrap_result (x : exec_result) : Mysql.result =
  match x with
      Result y -> y
    | Error s -> failwith s

let mysql_exec statement handler =
  call (fun env ->
    let dbd = Lazy.force env.dbd in
    let exec_result =
      try
        let result = Mysql.exec dbd statement in
        match Mysql.errmsg dbd with
            None -> Result result
          | Some s -> Error ("mysql error " ^ s)
      with
          Mysql.Error s -> Error ("mysql error " ^ s)
        | e ->
            Error
              ("exception raised by Mysql.exec: " ^ (Log.string_of_exn e))
    in
    (* Exceptions raised by handler are caught and logged in the worker
       because they can't be marshalled. *)
    handler exec_result
  ) >>= function
    | None ->
        (* Detailed error was already logged by the worker *)
        failwith ("mysql query via nproc failed: " ^ statement)
    | Some x -> return x

let test () =
  Lwt_main.run (
    mysql_exec "create database if not exists mytest;" ignore >>= fun () ->
    close_pool () >>= fun () ->
    return true
  )

let tests = [ "basic nproc test", test ]
