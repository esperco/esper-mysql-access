(*
  Unique keys associated with a value.

  Implementation of a functor 'Make' that takes as input the details
  of a key-value table (table name, key and value types,
  serialization functions) and returns type-safe operations
  for that table (get, get_exn, put, delete).

  See example of how to use the Make functor at the end of this file (test).
*)

module type KV_param =
sig
  val tblname : string
  module Key : Mysql_types.Serializable
  module Value : Mysql_types.Serializable
  module Ord : Mysql_types.Numeric
  val create_ord : Key.t -> Value.t -> Ord.t
  val update_ord : (Key.t -> Value.t -> Ord.t -> Ord.t) option
end

module type KV =
sig
  type key
  type value
  type ord

  val tblname : string
    (* Name of the MySQL table.
       The table must adhere to the schema called 'kv'
       (currently embedded in file 'create-kv-tbl.sql')
    *)

  val get : key -> (value * ord) option Lwt.t
    (* Get the value associated with the key if it exists. *)

  val put : key -> value -> unit Lwt.t
    (* Set the value associated with the key, overwriting the
       existing value if any. *)

  val mget : key list -> (key * value * ord) list Lwt.t
    (* Get multiple values *)

  val get_exn : key -> (value * ord) Lwt.t
    (* Get the value associated with the key, raising an exception
       if no such entry exists in the table. *)

  val update :
    key ->
    ((value * ord) option -> (value option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Set a write lock on the table/key pair,
       read the value and write the new value
       upon termination of the user-given function.
       If the returned value is [None] the original value is preserved.
    *)

  val update_exn :
    key ->
    ((value * ord) -> (value option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Same as [update] but raises an exception if the value does
       not exist initially. *)

  val create_exn : key -> value -> unit Lwt.t
    (* Same as [update] but raises an exception if a value already exists. *)

  val delete : key -> unit Lwt.t
    (* Set a write lock on the table/key pair and delete the entry
       if it exists. *)

  val lock : key -> (unit -> 'a Lwt.t) -> 'a Lwt.t
    (* Set a write lock on the table/key pair while the user-given
       function is running. [update] and [update_exn] are usually more useful.
    *)

  val unprotected_put : key -> value -> ord -> unit Lwt.t
    (* Add a new entry to the table or replace the existing one
       regardless of the presence of a lock. *)

  val unprotected_delete : key -> unit Lwt.t
    (* Remove the given entry if it exists regardless of the presence
       of a lock. *)

  (**/**)
  (* testing only; not the authoritative copy for this schema *)
  val create_table : unit -> unit Lwt.t

  val get_all : unit -> (value * ord) list Lwt.t
    (* Get all entries in the table, which can be a lot. *)
end


module Make (Param : KV_param) : KV
  with type key = Param.Key.t
  and type value = Param.Value.t
  and type ord = Param.Ord.t =


(*** Implementation ***)

struct
  open Printf
  open Lwt

  type key = Param.Key.t
  type value = Param.Value.t
  type ord = Param.Ord.t
  let tblname = Param.tblname

  let create_ord = Param.create_ord

  let update_ord =
    match Param.update_ord with
      None -> (fun k v ord -> ord)
    | Some f -> f

  let esc_tblname = Mysql.escape tblname
  let esc_key key = Mysql.escape (Param.Key.to_string key)
  let esc_value value = Mysql.escape (Param.Value.to_string value)
  let esc_ord ord =
    Mysql_types.ord_of_float (Param.Ord.to_float ord)

  let ord_of_string s =
    Param.Ord.of_float (float_of_string s)

  let key_not_found key =
    failwith (sprintf "Key '%s' not found in table '%s'"
                (esc_key key) esc_tblname)

  let key_exists key =
    failwith (sprintf "Key '%s' already exists in table '%s'"
                (esc_key key) esc_tblname)

  let get key =
    let st =
      sprintf "select v, ord from %s where k='%s';"
        esc_tblname (esc_key key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res = Mysql_lwt.unwrap_result x in
      (match Mysql.fetch res with
          Some [| Some v; Some ord |] ->
           Some (Param.Value.of_string v, ord_of_string ord)
        | None -> None
        | Some _ -> failwith ("Broken result returned on: " ^ st)
      )
    )

  let get_all () =
    let st =
      sprintf "select v, ord from %s;" esc_tblname
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res = Mysql_lwt.unwrap_result x in
      let rows = Mysql_util.fetch_all res in
      BatList.filter_map (function
        | [| Some v; Some ord |] ->
            (try Some (Param.Value.of_string v, ord_of_string ord)
             with _ -> None)
        |  _ -> failwith ("Broken result returned on: " ^ st)
        ) rows
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
            sprintf "select k, v, ord from %s where %s;" esc_tblname w
          in
          Mysql_lwt.mysql_exec st (fun x ->
            let res = Mysql_lwt.unwrap_result x in
            let rows = Mysql_util.fetch_all res in
            BatList.map (function
              | [| Some k; Some v; Some ord |] ->
                  (Param.Key.of_string k,
                   Param.Value.of_string v,
                   ord_of_string ord)
              |  _ -> failwith ("Broken result returned on: " ^ st)
            ) rows
          )

  let get_exn key =
    get key >>= function
      | None -> key_not_found key
      | Some x -> return x

  let unprotected_put key value ord =
    let st =
      sprintf "replace into %s(k, v, ord) values ('%s', '%s', %s);"
        esc_tblname (esc_key key) (esc_value value) (esc_ord ord)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let unprotected_delete key =
    let st =
      sprintf "delete from %s where k='%s';"
        esc_tblname (esc_key key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let mutex_name k =
    tblname ^ ":" ^ (Param.Key.to_string k)

  let lock k f =
    Redis_mutex.with_mutex (mutex_name k) f

  let update k f =
    lock k (fun () ->
      get k >>= fun opt_v_ord ->
      f opt_v_ord >>= fun (opt_v', result) ->
      (match opt_v' with
        | None -> return ()
        | Some v' ->
            let ord' =
              match opt_v_ord with
              | None -> create_ord k v'
              | Some (_, ord) -> update_ord k v' ord
            in
            unprotected_put k v' ord'
      ) >>= fun () ->
      return result
    )

  let update_exn k f =
    lock k (fun () ->
      get_exn k >>= fun v_ord ->
      f v_ord >>= fun (opt_v', result) ->
      (match opt_v' with
        | None -> return ()
        | Some v' ->
            let ord' = update_ord k v' (snd v_ord) in
            unprotected_put k v' ord'
      ) >>= fun () ->
      return result
    )

  let create_exn k v =
    update k (function
    | None -> return (Some v, ())
    | Some _ -> key_exists k)

  let put k v =
    update k (function _old ->
      return (Some v, ())
    )

  let delete k =
    lock k (fun () ->
      unprotected_delete k
    )

  (* NOT the authoritative copy of the schema. Do not use in production. *)
  let create_table () =
    let st =
      sprintf "\
create table if not exists %s (
       k varchar(767) character set ascii not null,
       v blob not null,
       ord double not null,

       primary key (k),
       index (ord)
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
    module Ord = struct
      type t = float
      let to_float x = x
      let of_float x = x
    end
    let create_ord k v = Unix.gettimeofday ()
    let update_ord = None
  end
  in
  let module Testkv = Make (Param) in
  let open Lwt in
  Lwt_main.run (
    Testkv.create_table () >>= fun () ->
    Random.self_init ();
    let k = Random.int 1_000_000 in
    let v = "abc" in
    let ord = sqrt 2. in
    Testkv.unprotected_put k v ord >>= fun () ->
    Testkv.get_exn k >>= fun (v', ord') ->
    assert (v' = v);
    assert (ord' = ord);
    let v2 = "def" in
    let ord2 = sqrt 3. in
    Testkv.unprotected_put k v2 ord2 >>= fun () ->
    Testkv.get_exn k >>= fun (v2', ord2') ->
    assert (v2' = v2);
    assert (ord2' = ord2);
    Testkv.unprotected_delete k >>= fun () ->
    Testkv.get k >>= fun opt_v_ord ->
    assert (opt_v_ord = None);
    return true
  )

let tests = [
  "basic key-value operations", test;
]
