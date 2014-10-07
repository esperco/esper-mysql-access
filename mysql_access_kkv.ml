(*
  Disjoint sets of values represented using one SQL table.
  This is suitable for representing one level of a hierarchy.

  Implementation of a functor 'Make' that takes as input the details
  of a table containing 3 columns:

  - k1: container key
  - k2: unique key
  - v: value associated with k2

  and returns type-safe operations for that table (get, get_exn, put, delete).

  See example of how to use the Make functor at the end of this file (test).
*)

module type KKV_param =
sig
  val tblname : string
  module Key1 : Mysql_types.Serializable
  module Key2 : Mysql_types.Serializable
  module Value : Mysql_types.Serializable
  module Ord : Mysql_types.Numeric
  val create_ord : Key1.t -> Key2.t -> Value.t -> Ord.t
  val update_ord : (Key1.t -> Key2.t -> Value.t -> Ord.t -> Ord.t) option
end

module type KKV =
sig
  type key1
  type key2
  type value
  type ord

  val tblname : string
    (* Name of the MySQL table.
       The table must adhere to the schema called 'kv'
       (currently embedded in file 'create-kv-tbl.sql')
    *)

  val get1 :
    ?ord_direction: Mysql_types.direction ->
    ?key2_direction: Mysql_types.direction ->
    ?xstart_key2: key2 ->
    ?min_ord: ord ->
    ?max_ord: ord ->
    ?max_count: int ->
    key1 -> (key2 * value * ord) list Lwt.t
    (*
       Get the values associated with the key, sorted by 'ord'
       then by 'key2'.
       If specified, 'xstart_key2' is the key after which the requested
       selection must start. It is intended for paging.
    *)

  val get1_page :
    ?ord_direction: Mysql_types.direction ->
    ?key2_direction: Mysql_types.direction ->
    ?xstart_key2: key2 ->
    ?min_ord: ord ->
    ?max_ord: ord ->
    ?max_count: int ->
    key1 -> ((key2 * value * ord) list * key2 option) Lwt.t
    (* Same as get1; returns the key to next page 'xstart_key2'
       for convenience *)

  val get2 : key2 -> (key1 * value * ord) option Lwt.t
    (* Get the value associated with the key if it exists. *)

  val put : key1 -> key2 -> value -> unit Lwt.t
    (* Set the container key and the value associated with the key key2,
       replacing the existing row if any. *)

  val mget2 : key2 list -> (key1 * key2 * value * ord) list Lwt.t
    (* Get multiple values *)

  val get2_exn : key2 -> (key1 * value * ord) Lwt.t
    (* Get the value associated with the key, raising an exception
       if no such entry exists in the table. *)

  val get_page :
    ?direction: Mysql_types.direction ->
    ?min_ord: ord ->
    ?max_ord: ord ->
    ?max_count: int ->
    unit -> (key1 * key2 * value * ord) list Lwt.t
    (* Select entries based on the 'ord' column.

       Note this is not ideal since 'ord' values are not guaranteed
       to be unique. If more than 'max_count' of them have the same
       values, some rows won't be retrievable with this function.
       Use get1_page for a better behavior.
    *)

  val update :
    key2 ->
    ((key1 * value * ord) option -> ((key1 * value) option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Set a write lock on the table/key pair,
       read the value and write the new value
       upon termination of the user-given function.
       If the returned value is [None] the original value is preserved.
    *)

  val update_exn :
    key2 ->
    ((key1 * value * ord) -> ((key1 * value) option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Same as [update] but raises an exception if the value does
       not exist initially. *)

  val delete1 : key1 -> unit Lwt.t
    (* Set a write lock on the table/key1 pair
       and delete the matching entries. *)

  val delete2 : key2 -> unit Lwt.t
    (* Set a write lock on the table/key2 pair and delete the entry
       if it exists. *)

  val lock1 : key1 -> (unit -> 'a Lwt.t) -> 'a Lwt.t
  val lock2 : key2 -> (unit -> 'a Lwt.t) -> 'a Lwt.t
    (* Set a write lock on the table/key pair while the user-given
       function is running. [update] and [update_exn] are usually more useful.
    *)

  val unprotected_put : key1 -> key2 -> value -> ord -> unit Lwt.t
    (* Add a new entry to the table or replace the existing one
       regardless of the presence of a lock. *)

  val unprotected_delete1 : key1 -> unit Lwt.t
    (* Remove the given entries if it exists regardless of the presence
       of a lock. *)

  val unprotected_delete2 : key2 -> unit Lwt.t
    (* Remove the given entry if it exists regardless of the presence
       of a lock. *)

  (**/**)
  (* testing only; not the authoritative copy for this schema *)
  val create_table : unit -> unit Lwt.t
end


module Make (Param : KKV_param) : KKV
  with type key1 = Param.Key1.t
  and type key2 = Param.Key2.t
  and type value = Param.Value.t
  and type ord = Param.Ord.t =


(*** Implementation ***)

struct
  open Printf
  open Lwt

  type key1 = Param.Key1.t
  type key2 = Param.Key2.t
  type value = Param.Value.t
  type ord = Param.Ord.t
  let tblname = Param.tblname

  let create_ord = Param.create_ord

  let update_ord =
    match Param.update_ord with
      None -> (fun k1 k2 v ord -> ord)
    | Some f -> f

  let esc_tblname = Mysql.escape tblname
  let esc_key1 key1 = Mysql.escape (Param.Key1.to_string key1)
  let esc_key2 key2 = Mysql.escape (Param.Key2.to_string key2)
  let esc_value value = Mysql.escape (Param.Value.to_string value)
  let esc_ord ord =
    Mysql_types.ord_of_float (Param.Ord.to_float ord)

  let ord_of_string s =
    Param.Ord.of_float (float_of_string s)

  let key2_not_found key2 =
    failwith (sprintf "Key '%s' not found in table '%s'"
                (esc_key2 key2) esc_tblname)

  let string_of_dir = function
    | `Asc -> "asc"
    | `Desc -> "desc"

  let get1
      ?(ord_direction = `Asc)
      ?(key2_direction = `Asc)
      ?xstart_key2
      ?min_ord ?max_ord ?max_count
      k1 =
    let mini =
      match min_ord with
      | None -> ""
      | Some x -> sprintf " and ord>=%s" (esc_ord x)
    in
    let maxi =
      match max_ord with
      | None -> ""
      | Some x -> sprintf " and ord<=%s" (esc_ord x)
    in
    let xstart =
      match key2_direction, xstart_key2 with
      | _, None -> ""
      | `Asc, Some k2 -> sprintf " and k2>'%s'" (esc_key2 k2)
      | `Desc, Some k2 -> sprintf " and k2<'%s'" (esc_key2 k2)
    in
    let order =
      sprintf " order by ord %s, k2 %s"
        (string_of_dir ord_direction)
        (string_of_dir key2_direction)
    in
    let limit =
      match max_count with
      | None -> ""
      | Some x -> sprintf " limit %d" x
    in
    let st =
      sprintf "select k2, v, ord from %s where k1='%s'%s%s%s%s%s;"
        esc_tblname
        (esc_key1 k1)
        mini
        maxi
        xstart
        order
        limit
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res = Mysql_lwt.unwrap_result x in
      let rows = Mysql_util.fetch_all res in
      BatList.filter_map (function
        | [| Some k2; Some v; Some ord |] ->
            (try Some (Param.Key2.of_string k2,
                       Param.Value.of_string v,
                       ord_of_string ord)
             with e ->
                 let msg = Log.string_of_exn e in
                 Log.logf `Error "Malformed row data in table %s: \
                              k1=%s k2=%s v=%s ord=%s exception: %s"
                   tblname
                   (Param.Key1.to_string k1) k2 v ord
                   msg;
                 None
            )
        |  _ -> failwith ("Broken result returned on: " ^ st)
      ) rows
    )

  let get2 key =
    let st =
      sprintf "select k1, v, ord from %s where k2='%s';"
        esc_tblname (esc_key2 key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res = Mysql_lwt.unwrap_result x in
      (match Mysql.fetch res with
          Some [| Some k1; Some v; Some ord |] ->
            Some (Param.Key1.of_string k1,
                  Param.Value.of_string v,
                  ord_of_string ord)
        | None -> None
        | Some _ -> failwith ("Broken result returned on: " ^ st)
      )
    )

  let get1_page
      ?ord_direction
      ?key2_direction
      ?xstart_key2
      ?min_ord ?max_ord ?max_count
      k1 =
    get1
      ?ord_direction
      ?key2_direction
      ?xstart_key2
      ?min_ord ?max_ord ?max_count
      k1 >>= fun l ->
    let next =
      match max_count with
      | Some m when List.length l < m -> None
      | _ ->
          match BatList.Exceptionless.last l with
          | None -> None
          | Some (k2, v, ord) -> Some k2
    in
    return (l, next)

  let mget2 keys =
    match keys with
        [] -> return []
      | _ ->
          let w =
            String.concat " or "
              (BatList.map (fun k -> sprintf "k2='%s'" (esc_key2 k)) keys)
          in
          let st =
            sprintf "select k1, k2, v, ord from %s where %s;" esc_tblname w
          in
          Mysql_lwt.mysql_exec st (fun x ->
            let res = Mysql_lwt.unwrap_result x in
            let rows = Mysql_util.fetch_all res in
            BatList.map (function
              | [| Some k1; Some k2; Some v; Some ord |] ->
                  (Param.Key1.of_string k1,
                   Param.Key2.of_string k2,
                   Param.Value.of_string v,
                   ord_of_string ord)
              |  _ -> failwith ("Broken result returned on: " ^ st)
            ) rows
          )

  let get2_exn key =
    get2 key >>= function
      | None -> key2_not_found key
      | Some x -> return x

  let get_page
      ?(direction = `Asc)
      ?min_ord
      ?max_ord
      ?max_count
      () =

    let mini =
      match min_ord with
      | None -> None
      | Some x -> Some (sprintf "ord>=%s" (esc_ord x))
    in
    let maxi =
      match max_ord with
      | None -> None
      | Some x -> Some (sprintf "ord<=%s" (esc_ord x))
    in
    let order =
      match direction with
      | `Asc -> " order by ord asc"
      | `Desc -> " order by ord desc"
    in
    let limit =
      match max_count with
      | None -> ""
      | Some x -> sprintf " limit %d" x
    in
    let where =
      let l = BatList.filter_map (fun o -> o) [mini; maxi] in
      match l with
      | [] -> ""
      | l -> " where " ^ String.concat " and " l
    in
    let st =
      sprintf "select k1, k2, v, ord from %s%s%s%s;"
        esc_tblname
        where
        order
        limit
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res = Mysql_lwt.unwrap_result x in
      let rows = Mysql_util.fetch_all res in
      BatList.filter_map (function
        | [| Some k1; Some k2; Some v; Some ord |] ->
            (try Some (Param.Key1.of_string k1,
                       Param.Key2.of_string k2,
                       Param.Value.of_string v,
                       ord_of_string ord)
             with e ->
                 let msg = Log.string_of_exn e in
                 Log.logf `Error "Malformed row data in table %s: \
                              k1=%s k2=%s v=%s ord=%s exception: %s"
                   tblname
                   k1 k2 v ord
                   msg;
                 None
            )
        |  _ -> failwith ("Broken result returned on: " ^ st)
        ) rows
    )

  let unprotected_put k1 k2 value ord =
    let st =
      sprintf "replace into %s(k1, k2, v, ord) values ('%s', '%s', '%s', %s);"
        esc_tblname (esc_key1 k1) (esc_key2 k2) (esc_value value) (esc_ord ord)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let unprotected_delete1 key1 =
    let st =
      sprintf "delete from %s where k1='%s';"
        esc_tblname (esc_key1 key1)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let unprotected_delete2 key2 =
    let st =
      sprintf "delete from %s where k2='%s';"
        esc_tblname (esc_key2 key2)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let mutex_name1 k =
    tblname ^ "::" ^ (Param.Key1.to_string k)

  let mutex_name2 k =
    tblname ^ ":" ^ (Param.Key2.to_string k)

  let lock1 k f =
    Redis_mutex.with_mutex (mutex_name1 k) f

  let lock2 k f =
    Redis_mutex.with_mutex (mutex_name2 k) f

  let update k2 f =
    lock2 k2 (fun () ->
      get2 k2 >>= fun opt_x ->
      f opt_x >>= fun (opt_v', result) ->
      (match opt_v' with
        | None -> return ()
        | Some (k1', v') ->
            let ord' =
              match opt_x with
              | None -> create_ord k1' k2 v'
              | Some (_k1, _v, ord) -> update_ord k1' k2 v' ord
            in
            unprotected_put k1' k2 v' ord'
      ) >>= fun () ->
      return result
    )

  let update_exn k2 f =
    lock2 k2 (fun () ->
      get2_exn k2 >>= fun x ->
      f x >>= fun (opt_v', result) ->
      (match opt_v' with
        | None -> return ()
        | Some (k1', v') ->
            let _k1, _v, ord = x in
            let ord' = update_ord k1' k2 v' ord in
            unprotected_put k1' k2 v' ord'
      ) >>= fun () ->
      return result
    )

  let put k1 k2 v =
    update k2 (fun _old ->
      return (Some (k1, v), ())
    )

  let delete1 k =
    lock1 k (fun () ->
      unprotected_delete1 k
    )

  let delete2 k =
    lock2 k (fun () ->
      unprotected_delete2 k
    )

  (* NOT the authoritative copy of the schema. Do not use in production. *)
  let create_table () =
    let st =
      sprintf "\
create table if not exists %s (
       k1 varchar(767) character set ascii not null,
       k2 varchar(767) character set ascii not null,
       v longblob not null,
       ord double not null,

       primary key (k2),
       index (k1),
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
    let tblname = "testkkv"
    module Key = struct
      type t = int
      let to_string = string_of_int
      let of_string = int_of_string
    end
    module Key1 = Key
    module Key2 = Key
    module Value = struct
      type t = string
      let to_string s = s
      let of_string s = s
    end
    module Ord = Util_time
    let create_ord = Mysql_types.KKV.created
    let update_ord = Mysql_types.KKV.last_modified
  end
  in
  let module Testkkv = Make (Param) in
  let open Lwt in
  Lwt_main.run (
    Testkkv.create_table () >>= fun () ->
    Random.self_init ();
    let k1 = Random.int 1_000_000 in
    let k21 = Random.int 1_000_000 in
    let k22 = Random.int 1_000_000 in
    let v1 = "abc" in
    let v2 = "def" in
    Testkkv.put k1 k21 v1 >>= fun () ->
    Testkkv.put k1 k22 v2 >>= fun () ->
    Testkkv.get2_exn k21 >>= fun (k1', v', ord') ->
    assert (k1' = k1);
    assert (v' = v1);
    Testkkv.get2_exn k22 >>= fun (k1', v', ord') ->
    assert (k1' = k1);
    assert (v' = v2);
    let v3 = "ggggg" in
    Testkkv.put k1 k21 v3 >>= fun () ->
    Testkkv.get2_exn k21 >>= fun (k1, v3', ord') ->
    assert (v3' = v3);
    Testkkv.get1 k1 >>= fun l ->
    assert (List.length l = 2);
    Testkkv.unprotected_delete2 k21 >>= fun () ->
    Testkkv.get2 k21 >>= fun opt_v ->
    assert (opt_v = None);
    Testkkv.unprotected_delete1 k1 >>= fun () ->
    Testkkv.get1 k1 >>= fun l ->
    assert (l = []);

    (* leave something in the table so we can look at it later *)
    Testkkv.put 12 34 "blob1" >>= fun () ->
    Testkkv.put 12 56 "blob2" >>= fun () ->
    Testkkv.put 34 78 "blob3" >>= fun () ->

    return true
  )

let tests = [
  "basic kkv operations", test;
]
