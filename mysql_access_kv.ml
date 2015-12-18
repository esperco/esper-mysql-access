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

  val get : key -> value option Lwt.t
    (* Get the value associated with the key if it exists. *)

  val get_exn : key -> value Lwt.t
    (* Get the value associated with the key, raising an exception
       if no such entry exists in the table. *)

  val get_full : key -> (value * ord) option Lwt.t
    (* Same as [get] but returns the 'ord' field as well. *)

  val exists: key -> bool Lwt.t

  val count : unit -> int Lwt.t
    (* Count the number of rows in the table using mysql count()
       TODO: add range filter on ord
    *)

  val to_stream :
    ?page_size: int ->
    ?min_ord: ord ->
    ?xmin_ord: ord ->
    ?max_ord: ord ->
    ?xmax_ord: ord ->
    unit -> (key * value * ord) Lwt_stream.t
    (* get all entries in the table, page by page.
       page_size is the number of items to fetch in one mysql query
       (default: 1000)
    *)

  val to_list :
    ?page_size: int ->
    ?min_ord: ord ->
    ?xmin_ord: ord ->
    ?max_ord: ord ->
    ?xmax_ord: ord ->
    unit -> (key * value * ord) list Lwt.t
    (* get all entries in the table as a list.
       Not recommended on large tables.
    *)

  val iter :
    ?page_size: int ->
    ?min_ord: ord ->
    ?xmin_ord: ord ->
    ?max_ord: ord ->
    ?xmax_ord: ord ->
    ?max_threads: int ->
    ((key * value * ord) -> unit Lwt.t) ->
    unit Lwt.t
    (* iterate over all entries in the table sequentially.
       page_size is the number of items to fetch in one mysql query
       (default: 1000).
       max_threads is the maximum number of items to process in parallel
       (default: 1, i.e. sequential processing is the default).
    *)

  val put : key -> value -> unit Lwt.t
    (* Set the value associated with the key, overwriting the
       existing value if any. *)

  val mget : key list -> (key * value * ord) list Lwt.t
    (* Get multiple values. Key order is preserved. *)

  val update :
    key ->
    (value option -> (value option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Set a write lock on the table/key pair,
       read the value and write the new value
       upon termination of the user-given function.
       If the returned value is [None] the original value is preserved.
    *)

  val update_exn :
    key ->
    (value -> (value option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Same as [update] but raises an exception if the value does
       not exist initially. *)

  val update_full :
    key ->
    ((value * ord) option -> (value option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Same as [update] but [ord] is also passed to the callback function *)

  val create_exn : key -> value -> unit Lwt.t
    (* Same as [update] but raises an exception if a value already exists. *)

  val delete : key -> unit Lwt.t
    (* Set a write lock on the table/key pair and delete the entry
       if it exists. *)

  val delete_range :
    ?key:key ->
    ?max_count:int ->
    ?min_ord:ord ->
    ?xmin_ord:ord ->
    ?max_ord:ord ->
    ?xmax_ord:ord ->
    unit -> int64 Lwt.t
    (* Delete multiple rows in a single mysql call,
       return the number of rows deleted. *)

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

  let get_full key =
    let st =
      sprintf "select v, ord from %s where k='%s';"
        esc_tblname (esc_key key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res, _affected = Mysql_lwt.unwrap_result x in
      (match Mysql.fetch res with
          Some [| Some v; Some ord |] ->
           Some (Param.Value.of_string v, ord_of_string ord)
        | None -> None
        | Some _ -> failwith ("Broken result returned on: " ^ st)
      )
    )

  let get key =
    let st =
      sprintf "select v from %s where k='%s';"
        esc_tblname (esc_key key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res, _affected = Mysql_lwt.unwrap_result x in
      (match Mysql.fetch res with
          Some [| Some v |] ->
           Some (Param.Value.of_string v)
        | None -> None
        | Some _ -> failwith ("Broken result returned on: " ^ st)
      )
    )

  let get_exn key =
    get key >>= function
      | None -> key_not_found key
      | Some x -> return x

  let exists key =
    get key >>= function
    | None   -> return false
    | Some _ -> return true

  let count () =
    let st =
      sprintf "select count(*) from %s;"
        esc_tblname
    in
    Mysql_lwt.mysql_exec st (fun x ->
      match Mysql.fetch (fst (Mysql_lwt.unwrap_result x)) with
      | Some [| Some n |] -> int_of_string n
      | _ -> failwith ("Broken result returned on: " ^ st)
    )

  let make_where_clause
    ?key
    ?after
    ?min_ord
    ?xmin_ord
    ?max_ord
    ?xmax_ord
    () =
    let key_clause =
      match after with
      | Some k -> [ sprintf "k > '%s'" (esc_key k) ]
      | None ->
          match key with
          | None -> []
          | Some k -> [ sprintf "k = '%s'" (esc_key k) ]
    in
    let min_ord_clause =
      match min_ord, xmin_ord with
      | None, None -> []
      | Some ord, None -> [ sprintf "ord >= %s" (esc_ord ord) ]
      | _, Some ord -> [ sprintf "ord > %s" (esc_ord ord) ]
    in
    let max_ord_clause =
      match max_ord, xmax_ord with
      | None, None -> []
      | Some ord, None -> [ sprintf "ord <= %s" (esc_ord ord) ]
      | _, Some ord -> [ sprintf "ord < %s" (esc_ord ord) ]
    in
    let filters =
      List.flatten [ key_clause; min_ord_clause; max_ord_clause ]
    in
    let where =
      match filters with
      | [] -> ""
      | l -> " where " ^ String.concat " and " l
    in
    where

  let make_limit_clause opt_max_count =
    match opt_max_count with
    | None -> ""
    | Some n when n > 0 -> sprintf " limit %d" n
    | Some n -> invalid_arg (sprintf "max_count:%d" n)

  let rec get_page
      ?after
      ?max_count
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord (): ('a list * 'a Mysql_util.get_page option) Lwt.t =
    let where =
      make_where_clause
        ?after
        ?min_ord
        ?xmin_ord
        ?max_ord
        ?xmax_ord
        ()
    in
    let limit = make_limit_clause max_count in
    let st =
      sprintf "select k, v, ord from %s%s order by k asc%s;"
        esc_tblname
        where
        limit
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res, _affected = Mysql_lwt.unwrap_result x in
      let rows = Mysql_util.fetch_all res in
      let page =
        BatList.map (function
          | [| Some k; Some v; Some ord |] ->
              (Param.Key.of_string k,
               Param.Value.of_string v,
               ord_of_string ord)
          |  _ -> failwith ("Broken result returned on: " ^ st)
        ) rows
      in
      let next =
        match page with
        | [] -> None
        | l ->
            let k, _, _ = BatList.last l in
            Some (`Get_page (fun () ->
              get_page
                ~after:k
                ?max_count
                ?min_ord
                ?xmin_ord
                ?max_ord
                ?xmax_ord
                ()
            ))
      in
      page, next
    )

  let to_stream
      ?(page_size = 1000)
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord
      () =
    let get_first_page () =
      get_page
        ~max_count:page_size
        ?min_ord
        ?xmin_ord
        ?max_ord
        ?xmax_ord ()
    in
    Mysql_util.stream_from_pages get_first_page

  let iter
      ?page_size
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord
      ?(max_threads = 1) f =
    let stream =
      to_stream
        ?page_size
        ?min_ord
        ?xmin_ord
        ?max_ord
        ?xmax_ord
        ()
    in
    Util_lwt.iter_stream max_threads stream f

  let to_list
      ?page_size
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord () =
    Lwt_stream.to_list (to_stream
                          ?page_size
                          ?min_ord
                          ?xmin_ord
                          ?max_ord
                          ?xmax_ord
                          ()
                       )

  let mget keys =
    match keys with
        [] -> return []
      | _ ->
          let keys_s = BatList.map esc_key keys in
          let w =
            String.concat " or "
              (BatList.map (fun k_s -> sprintf "k='%s'" k_s) keys_s)
          in
          let st =
            sprintf "select k, v, ord from %s where %s;" esc_tblname w
          in
          Mysql_lwt.mysql_exec st (fun x ->
            let res, _affected = Mysql_lwt.unwrap_result x in
            let rows = Mysql_util.fetch_all res in
            let unordered =
              BatList.map (function
                | [| Some k; Some v; Some ord |] ->
                    (k, (Param.Key.of_string k,
                         Param.Value.of_string v,
                         ord_of_string ord))
                |  _ -> failwith ("Broken result returned on: " ^ st)
              ) rows
            in
            let ordered = Mysql_util.reorder keys_s unordered fst in
            BatList.map snd ordered
          )

  let unprotected_put key value ord =
    let st =
      sprintf "replace into %s(k, v, ord) values ('%s', '%s', %s);"
        esc_tblname (esc_key key) (esc_value value) (esc_ord ord)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let delete_range
    ?key
    ?max_count
    ?min_ord
    ?xmin_ord
    ?max_ord
    ?xmax_ord
    () =
    let where =
      make_where_clause
        ?key
        ?min_ord
        ?xmin_ord
        ?max_ord
        ?xmax_ord
        ()
    in
    let limit = make_limit_clause max_count in
    let st =
      sprintf "delete from %s%s%s;" esc_tblname where limit
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let (res, count) = Mysql_lwt.unwrap_result x in
      count
    )

  let unprotected_delete key =
    delete_range ~key () >>= fun n ->
    return ()

  let mutex_name k =
    tblname ^ ":" ^ (Param.Key.to_string k)

  let lock k f =
    Redis_mutex.with_mutex (mutex_name k) f

  let update_full k f =
    lock k (fun () ->
      get_full k >>= fun opt_v_ord ->
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

  let update k f =
    update_full k (function
      | None -> f None
      | Some (v, ord) -> f (Some v)
    )

  let update_exn k f =
    lock k (fun () ->
      get_full k >>= function
      | None -> key_not_found k
      | Some (v, ord) ->
          f v >>= fun (opt_v', result) ->
          (match opt_v' with
           | None -> return ()
           | Some v' ->
               let ord' = update_ord k v' ord in
               unprotected_put k v' ord'
          ) >>= fun () ->
          return result
    )

  let create_exn k v =
    update k (function
      | None -> return (Some v, ())
      | Some _ -> key_exists k
    )

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
       v longblob not null,
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
  Util_lwt_main.run (
    Testkv.create_table () >>= fun () ->
    Random.self_init ();
    let k = Random.int 1_000_000 in
    let v = "abc" in
    let ord = sqrt 2. in
    Testkv.unprotected_put k v ord >>= fun () ->
    Testkv.get_exn k >>= fun v' ->
    assert (v' = v);
    let v2 = "def" in
    let ord2 = sqrt 3. in
    Testkv.unprotected_put k v2 ord2 >>= fun () ->
    Testkv.get_exn k >>= fun v2' ->
    assert (v2' = v2);
    Testkv.unprotected_delete k >>= fun () ->
    Testkv.get k >>= fun opt_v ->
    assert (opt_v = None);
    return true
  )

let test_kv_paging () =
  let module Param = struct
    let tblname = "testkv_paging"
    module Key = struct
      type t = int
      let to_string = string_of_int
      let of_string = int_of_string
    end
    module Value = struct
      type t = float
      let to_string x = Printf.sprintf "%.17g" x
      let of_string s = float_of_string s
    end
    module Ord = struct
      type t = float
      let to_float x = x
      let of_float x = x
    end
    let create_ord k v = v
    let update_ord = None
  end
  in
  let module Tbl = Make (Param) in
  let open Lwt in
  Util_lwt_main.run (
    Tbl.create_table () >>= fun () ->
    let data = [
      1, 1.;
      2, 1.;
      3, 20.;
      4, 1.;
      5, 1.;
      6, 1.;
    ] in
    let sorted_keys = List.map (fun (k, v) -> k) data in
    Lwt_list.iter_s (fun (k, v) ->
      Tbl.put k v
    ) data >>= fun () ->

    Tbl.to_list ~page_size:3 () >>= fun l ->
    let extracted_keys = List.map (fun (k, v, ord) -> k) l in
    assert (extracted_keys = sorted_keys);

    Tbl.to_list ~min_ord:1. ~xmin_ord:1. () >>= fun l ->
    assert (List.length l = 1);

    Tbl.to_list ~xmin_ord:1. ~max_ord:20. () >>= fun l ->
    assert (List.length l = 1);

    Tbl.to_list ~xmin_ord:1. ~xmax_ord:20. () >>= fun l ->
    assert (List.length l = 0);

    Tbl.delete_range ~min_ord:0.5 ~xmax_ord:20. () >>= fun n ->
    assert (n = 5L);

    Tbl.to_list () >>= fun l ->
    assert (List.length l = 1);

    return true
  )

let tests = [
  "basic key-value operations", test;
  "kv paging", test_kv_paging;
]
