(*
  Implementation of a functor 'Make' that takes as input the details
  of a key-value table (table name, key and value types,
  serialization functions) and returns type-safe operations
  for that table (get, get_exn, put, delete).

  See example of how to use the Make functor at the end of this file (test).
*)

module type Serializable =
sig
  type t
  val to_string : t -> string
  val of_string : string -> t
end

module type KV_param =
sig
  val tblname : string
  module Key : Serializable
  module Value : Serializable
end

module type KV =
sig
  type key
  type value

  val tblname : string
    (* Name of the MySQL table.
       The table must adhere to the schema called 'kv'
       (currently embedded in file 'create-kv-tbl.sql')
    *)

  val get : key -> value option Lwt.t
    (* Get the value associated with the key if it exists. *)

  val get_exn : key -> value Lwt.t
    (* Get the value associated with the key, raising an exception
       if no such entry exists in the table. *)

  val put : key -> value -> unit Lwt.t
    (* Add a new entry to the table or replace the existing one. *)

  val delete : key -> unit Lwt.t
    (* Remove the given entry if it exists. *)

  (**/**)
  (* testing only; not the authoritative copy for this schema *)
  val create_table : unit -> unit Lwt.t
end


module Make (Param : KV_param) : KV
  with type key = Param.Key.t
  and type value = Param.Value.t =


(*** Implementation ***)

struct
  open Printf
  open Lwt

  type key = Param.Key.t
  type value = Param.Value.t
  let tblname = Param.tblname
  let esc_tblname = Mysql.escape tblname
  let esc_key key = Mysql.escape (Param.Key.to_string key)
  let esc_value value = Mysql.escape (Param.Value.to_string value)

  let key_not_found key =
    failwith (sprintf "Key '%s' not found in table '%s'"
                (esc_key key) esc_tblname)

  let get key =
    let st =
      sprintf "select v from %s where k='%s';"
        esc_tblname (esc_key key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res = Mysql_lwt.unwrap_result x in
      (match Mysql.fetch res with
          Some [| Some v |] -> Some (Param.Value.of_string v)
        | None -> None
        | Some _ -> failwith ("Broken result returned on: " ^ st)
      )
    )

  let get_exn key =
    get key >>= function
      | None -> key_not_found key
      | Some x -> return x

  let put key value =
    let st =
      sprintf "replace into %s(k, v) values ('%s', '%s');"
        esc_tblname (esc_key key) (esc_value value)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let delete key =
    let st =
      sprintf "delete from %s where k='%s';"
        esc_tblname (esc_key key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  (* NOT the authoritative copy of the schema. Do not use in production. *)
  let create_table () =
    let st =
      sprintf "\
create table if not exists %s (
    k varchar(767) character set ascii primary key,
    v blob
) engine=InnoDB;
"
        esc_tblname
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )
end

let test () =
  let module Param = struct
    let tblname = "testkv"
    module Key = struct
      type t = int
      let to_string = string_of_int
      let of_string = int_of_string
    end
    module Value = struct
      type t = string
      let to_string s = s
      let of_string s = s
    end
  end
  in
  let module Testkv = Make (Param) in
  let open Lwt in
  Lwt_main.run (
    Testkv.create_table () >>= fun () ->
    Random.self_init ();
    let k = Random.int 1_000_000 in
    let v = "abc" in
    Testkv.put k v >>= fun () ->
    Testkv.get_exn k >>= fun v' ->
    assert (v' = v);
    let v2 = "def" in
    Testkv.put k v2 >>= fun () ->
    Testkv.get_exn k >>= fun v2' ->
    assert (v2' = v2);
    Testkv.delete k >>= fun () ->
    Testkv.get k >>= fun opt_v ->
    assert (opt_v = None);
    return true
  )

let tests = [
  "basic key-value operations", test;
]
