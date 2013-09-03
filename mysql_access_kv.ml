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

  val mget : key list -> (key * value) list Lwt.t
    (* Get multiple values *)

  val get_exn : key -> value Lwt.t
    (* Get the value associated with the key, raising an exception
       if no such entry exists in the table. *)

  val update : key -> (value option -> value Lwt.t) -> value Lwt.t
    (* Set a write lock on the table/key pair,
       read the value and write the new value
       upon termination of the user-given function. *)

  val update_exn : key -> (value -> value Lwt.t) -> value Lwt.t
    (* Same as [update] but raises an exception if the value does
       not exist initially. *)

  val with_lock : key -> (unit -> 'a Lwt.t) -> 'a Lwt.t
    (* Set a write lock on the table/key pair while the user-given
       function is running. [update] and [update_exn] are usually more useful.
       Deleting a value safely is one use of this function.
    *)

  val unsafe_put : key -> value -> unit Lwt.t
    (* Add a new entry to the table or replace the existing one
       regardless of the presence of a lock. *)

  val unsafe_delete : key -> unit Lwt.t
    (* Remove the given entry if it exists regardless of the presence
       of a lock. *)

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

  let mget keys =
    match keys with
        [] -> return []
      | _ ->
          let w =
            String.concat " or "
              (BatList.map (fun k -> sprintf "k='%s'" (esc_key k)) keys)
          in
          let st =
            sprintf "select k, v from %s where %s;" esc_tblname w
          in
          Mysql_lwt.mysql_exec st (fun x ->
            let res = Mysql_lwt.unwrap_result x in
            let rows = Mysql_util.fetch_all res in
            BatList.map (function
              | [| Some k; Some v |] ->
                  (Param.Key.of_string k, Param.Value.of_string v)
              |  _ -> failwith ("Broken result returned on: " ^ st)
            ) rows
          )

  let get_exn key =
    get key >>= function
      | None -> key_not_found key
      | Some x -> return x

  let unsafe_put key value =
    let st =
      sprintf "replace into %s(k, v) values ('%s', '%s');"
        esc_tblname (esc_key key) (esc_value value)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let unsafe_delete key =
    let st =
      sprintf "delete from %s where k='%s';"
        esc_tblname (esc_key key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let with_lock k f = (* TODO not implemented *)
    f ()

  let update k f =
    with_lock k (fun () ->
      get k >>= fun opt_v ->
      f opt_v >>= fun v' ->
      unsafe_put k v' >>= fun () ->
      return v'
    )

  let update_exn k f =
    with_lock k (fun () ->
      get_exn k >>= fun v ->
      f v >>= fun v' ->
      unsafe_put k v' >>= fun () ->
      return v'
    )

  (* NOT the authoritative copy of the schema. Do not use in production. *)
  let create_table () =
    let st =
      sprintf "\
create table if not exists %s (
       k varchar(767) character set ascii not null,
       v blob not null,

       primary key (k)
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
    Testkv.unsafe_put k v >>= fun () ->
    Testkv.get_exn k >>= fun v' ->
    assert (v' = v);
    let v2 = "def" in
    Testkv.unsafe_put k v2 >>= fun () ->
    Testkv.get_exn k >>= fun v2' ->
    assert (v2' = v2);
    Testkv.unsafe_delete k >>= fun () ->
    Testkv.get k >>= fun opt_v ->
    assert (opt_v = None);
    return true
  )

let tests = [
  "basic key-value operations", test;
]
