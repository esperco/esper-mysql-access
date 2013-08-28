(*
  Asynchronous access to MySQL relying on a pool of processes,
  each in charge of one database connection.
*)

open Printf

type worker_env = Mysql.dbd Lazy.t

(* called only in the worker *)
let connect () =
  printf "connect\n%!";
  let db = Mysql.defaults in
  Mysql.connect db

let env : worker_env = lazy (connect ())

let call =
  let lazy_pool = lazy (
    Nproc.Full.create 10
      (fun () -> Lwt.return ())
      env
  ) in
  let pool () =
    fst (Lazy.force lazy_pool)
  in
  let call user_f =
    let f service env () =
      let dbd = Lazy.force env in
      user_f dbd
    in
    let p = pool () in
    printf "submit\n%!";
    Nproc.Full.submit p f ()
  in
  call

let test () =
  let result =
    Lwt_main.run (
      call (fun dbd ->
        1 + 1
        (*Mysql.exec dbd "create database mytest;"*)
      )
    )
  in
  result = Some 2

let tests = [ "basic nproc test", test ]
