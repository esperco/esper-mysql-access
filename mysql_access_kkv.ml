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
    unit -> (key1 * key2 * value * ord) Lwt_stream.t
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
    unit -> (key1 * key2 * value * ord) list Lwt.t
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
    ((key1 * key2 * value * ord) -> unit Lwt.t) ->
    unit Lwt.t
    (* iterate over all entries in the table sequentially.
       page_size is the number of items to fetch in one mysql query
       (default: 1000).
       max_threads is the maximum number of items to process in parallel
       (default: 1, i.e. sequential processing is the default).
    *)

  val get1 :
    ?ord_direction: Mysql_types.direction ->
    ?key2_direction: Mysql_types.direction ->
    ?start_ord: ord ->
    ?xstart_key2: key2 ->
    ?xend_ord: ord ->
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
    ?start_ord: ord ->
    ?xstart_key2: key2 ->
    ?xend_ord: ord ->
    ?max_count: int ->
    key1 -> ((key2 * value * ord) list * (ord * key2) option) Lwt.t
    (* Same as get1; returns the keys to next page ('start_ord', 'xstart_key2')
       for convenience *)

  val to_stream1 :
    ?page_size: int ->
    ?ord_direction: Mysql_types.direction ->
    ?key2_direction: Mysql_types.direction ->
    ?start_ord: ord ->
    ?xend_ord: ord ->
    ?max_count: int ->
    key1 -> (key2 * value * ord) Lwt_stream.t

  val iter1 :
    ?page_size: int ->
    ?ord_direction: Mysql_types.direction ->
    ?key2_direction: Mysql_types.direction ->
    ?start_ord: ord ->
    ?xend_ord: ord ->
    ?max_count: int ->
    ?max_threads: int ->
    key1 -> ((key2 * value * ord) -> unit Lwt.t) -> unit Lwt.t

  val get2 : key2 -> value option Lwt.t
    (* Get the value associated with the key if it exists. *)

  val get2_full : key2 -> (key1 * value * ord) option Lwt.t
    (* Get the value associated with the key if it exists. *)

  val exists2 : key2 -> bool Lwt.t
    (* Whether such entry exists *)

  val put : key1 -> key2 -> value -> unit Lwt.t
    (* Set the container key and the value associated with the key key2,
       replacing the existing row if any. *)

  val mget2 : key2 list -> (key1 * key2 * value * ord) list Lwt.t
    (* Get multiple values. Key order is preserved. *)

  val get2_exn : key2 -> (key1 * value * ord) Lwt.t
    (* Get the value associated with the key, raising an exception
       if no such entry exists in the table. *)

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
    | `Asc -> ""
    | `Desc -> " desc"

  let lt_of_dir = function
    | `Asc -> "<"
    | `Desc -> ">"

  let gt_of_dir = function
    | `Asc -> ">"
    | `Desc -> "<"


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

  let rec get_page
      ?after
      ?max_count
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord (): ('a list * 'a Mysql_util.get_page option) Lwt.t =
    let after_clause =
      match after with
      | None -> []
      | Some k2 -> [ sprintf "k2 > '%s'" (esc_key2 k2) ]
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
      List.flatten [ after_clause; min_ord_clause; max_ord_clause ]
    in
    let where =
      match filters with
      | [] -> ""
      | l -> " where " ^ String.concat " and " l
    in
    let limit =
      match max_count with
      | None -> ""
      | Some n when n > 0 -> sprintf " limit %d" n
      | Some n -> invalid_arg (sprintf "get_page ~max_count:%d" n)
    in
    let st =
      sprintf "select k1, k2, v, ord from %s%s order by k2 asc%s;"
        esc_tblname
        where
        limit
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res, _affected = Mysql_lwt.unwrap_result x in
      let rows = Mysql_util.fetch_all res in
      let page =
        BatList.map (function
          | [| Some k1; Some k2; Some v; Some ord |] ->
              (Param.Key1.of_string k1, Param.Key2.of_string k2,
               Param.Value.of_string v, ord_of_string ord)
          |  _ -> failwith ("Broken result returned on: " ^ st)
        ) rows
      in
      let next =
        match page with
        | [] -> None
        | l ->
            let k1, k2, _, _ = BatList.last l in
            Some (`Get_page (fun () ->
              get_page
                ~after:k2
                ?max_count
                ?min_ord
                ?xmin_ord
                ?max_ord
                ?xmax_ord
                ())
            )
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
        ?xmax_ord
        ()
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

  let get1
      ?(ord_direction = `Asc)
      ?(key2_direction = `Asc)
      ?start_ord
      ?xstart_key2
      ?xend_ord
      ?max_count
      k1 =
    let start =
      match start_ord, xstart_key2 with
      | None, None -> ""
      | None, Some _ ->
          invalid_arg
            "Mysql_access_kkv.get1: meaningless parameter \
                                    xstart_key2 without start_ord"
      | Some ord, None ->
          sprintf " and ord %s= %s"
            (gt_of_dir ord_direction)
            (esc_ord ord)
      | Some ord, Some k2 ->
          let ord_s = esc_ord ord in
          sprintf " and (ord %s %s or ord = %s and k2 %s '%s')"
            (gt_of_dir ord_direction)
            ord_s
            ord_s
            (gt_of_dir key2_direction)
            (esc_key2 k2)
    in
    let end_ =
      match xend_ord with
      | None -> ""
      | Some ord ->
          sprintf " and ord %s %s"
            (lt_of_dir ord_direction)
            (esc_ord ord)
    in
    let order =
      sprintf " order by ord%s, k2%s"
        (string_of_dir ord_direction)
        (string_of_dir key2_direction)
    in
    let limit =
      match max_count with
      | None -> ""
      | Some x -> sprintf " limit %d" x
    in
    let st =
      sprintf "select k2, v, ord from %s where k1 = '%s'%s%s%s%s;"
        esc_tblname
        (esc_key1 k1)
        start
        end_
        order
        limit
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res, _affected = Mysql_lwt.unwrap_result x in
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

  let get2_full key =
    let st =
      sprintf "select k1, v, ord from %s where k2='%s';"
        esc_tblname (esc_key2 key)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res, _affected = Mysql_lwt.unwrap_result x in
      (match Mysql.fetch res with
          Some [| Some k1; Some v; Some ord |] ->
            Some (Param.Key1.of_string k1,
                  Param.Value.of_string v,
                  ord_of_string ord)
        | None -> None
        | Some _ -> failwith ("Broken result returned on: " ^ st)
      )
    )

  let get2 k2 =
    get2_full k2 >>= function
    | None -> return None
    | Some (k1, v, ord) -> return (Some v)

  let exists2 k2 =
    get2 k2 >>= function
    | None -> return false
    | Some _ -> return true

  let get1_page
      ?ord_direction
      ?key2_direction
      ?start_ord
      ?xstart_key2
      ?xend_ord
      ?max_count
      k1 =
    get1
      ?ord_direction
      ?key2_direction
      ?start_ord
      ?xstart_key2
      ?xend_ord
      ?max_count
      k1 >>= fun l ->
    let next =
      match max_count with
      | None -> None
      | Some m when List.length l < m -> None
      | _ ->
          match BatList.Exceptionless.last l with
          | None -> None
          | Some (k2, v, ord) -> Some (ord, k2)
    in
    return (l, next)

  let to_stream1
      ?(page_size = 1000)
      ?ord_direction
      ?key2_direction
      ?start_ord
      ?xend_ord
      ?max_count
      k1 =

    let rec get_page start_ord xstart_key2 () =
      get1_page
        ?ord_direction
        ?key2_direction
        ?start_ord
        ?xstart_key2
        ?xend_ord
        ~max_count:page_size
        k1
      >>= fun (page, last) ->
      let next =
        match last with
        | None ->
            None
        | Some (last_ord, last_k2) ->
            Some (`Get_page (get_page (Some last_ord) (Some last_k2)))
      in
      return (page, next)
    in
    Mysql_util.stream_from_pages (get_page None None)

  let iter1
    ?page_size
    ?ord_direction
    ?key2_direction
    ?start_ord
    ?xend_ord
    ?max_count
    ?(max_threads = 1)
    key1 f =

    let stream =
      to_stream1
        ?page_size
        ?ord_direction
        ?key2_direction
        ?start_ord
        ?xend_ord
        ?max_count
        key1
    in
    Util_lwt.iter_stream max_threads stream f

  let mget2 keys =
    match keys with
        [] -> return []
      | _ ->
          let keys_s = BatList.map esc_key2 keys in
          let w =
            String.concat " or "
              (BatList.map (fun k_s -> sprintf "k2='%s'" k_s) keys_s)
          in
          let st =
            sprintf "select k1, k2, v, ord from %s where %s;" esc_tblname w
          in
          Mysql_lwt.mysql_exec st (fun x ->
            let res, _affected = Mysql_lwt.unwrap_result x in
            let rows = Mysql_util.fetch_all res in
            let unordered =
              BatList.map (function
                | [| Some k1; Some k2; Some v; Some ord |] ->
                    (k2, (Param.Key1.of_string k1,
                          Param.Key2.of_string k2,
                          Param.Value.of_string v,
                          ord_of_string ord)
                    )
                |  _ -> failwith ("Broken result returned on: " ^ st)
              ) rows
            in
            let ordered = Mysql_util.reorder keys_s unordered fst in
            BatList.map snd ordered
          )

  let get2_exn key =
    get2_full key >>= function
      | None -> key2_not_found key
      | Some x -> return x

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
      get2_full k2 >>= fun opt_x ->
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

let test_kkv_basic_operations () =
  let module Param = struct
    let tblname = "testkkv_basic"
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
  Util_lwt_main.run (
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

let test_kkv_paging () =
  let module Param = struct
    let tblname = "testkkv_paging"
    module Key = struct
      type t = int
      let to_string = string_of_int
      let of_string = int_of_string
    end
    module Key1 = Key
    module Key2 = Key
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
    let create_ord k1 k2 v = v
    let update_ord = None
  end
  in
  let module Tbl = Make (Param) in
  let open Lwt in
  Util_lwt_main.run (
    Tbl.create_table () >>= fun () ->
    let data = [
      0, 1, 1.;
      1, 2, 1.;
      1, 3, 20.;
      1, 4, 1.;
      1, 5, 1.;
      2, 6, 1.;
    ] in
    let sorted_keys = List.map (fun (k1, k2, v) -> (k1, k2)) data in
    Lwt_list.iter_s (fun (k1, k2, v) ->
      Tbl.put k1 k2 v
    ) data >>= fun () ->

    (Tbl.get1_page 0 >>= function
    | [ (1, v, ord) ], None ->
        assert (v = ord);
        return ()
    | _ :: _, _ ->
        assert false
    | _, Some _ ->
        assert false
    | _ ->
        assert false
    ) >>= fun () ->

    (Tbl.get1_page 1 >>= function
    | [
        2, v2, ord2;
        4, v4, ord4;
        5, v5, ord5;
        3, v3, ord3;
      ], None ->
        assert (v2 = ord2);
        assert (v3 = ord3);
        assert (v4 = ord4);
        assert (v5 = ord5);
        return ()
    | _, Some _ ->
        assert false
    | l, None ->
        assert (List.length l = 4);
        assert false
    ) >>= fun () ->

    (Tbl.get1_page 1 ~max_count:2 ~ord_direction:`Desc >>= function
    | [
        3, v3, ord3;
        2, v2, ord2;
      ], Some (start_ord, xstart_key2) ->

        (Tbl.get1_page 1
           ~max_count:2 ~ord_direction:`Desc
           ~start_ord ~xstart_key2 >>= function
         | [
             4, v4, ord4;
             5, v5, ord5;
           ], Some (start_ord, xstart_key2) ->

             (Tbl.get1_page 1
                ~max_count:2 ~ord_direction:`Desc
                ~start_ord ~xstart_key2 >>= function
              | [], None -> return ()
              | _ :: _, None -> assert false
              | _, Some _ -> assert false
             )
         | _, Some _ -> assert false
         | _, None -> assert false
        )
    | _, Some _ ->
        assert false
    | l, None ->
        assert (List.length l = 4);
        assert false
    ) >>= fun () ->

    Tbl.to_list ~page_size:3 () >>= fun l ->
    let extracted_keys = List.map (fun (k1, k2, v, ord) -> (k1, k2)) l in
    assert (extracted_keys = sorted_keys);

    Tbl.to_list ~min_ord:1. ~xmin_ord:1. () >>= fun l ->
    assert (List.length l = 1);

    Tbl.to_list ~xmin_ord:1. ~max_ord:20. () >>= fun l ->
    assert (List.length l = 1);

    Tbl.to_list ~xmin_ord:1. ~xmax_ord:20. () >>= fun l ->
    assert (List.length l = 0);

    let stream =
      Tbl.to_stream1 1
        ~page_size:2 ~max_count:3 ~ord_direction:`Desc
    in
    Lwt_stream.to_list stream >>= fun l ->
    (match l with
     | [ 3, _, 20.;
         2, _, 1.;
         4, _, 1.;
         5, _, 1.;
       ] -> ()
     | _ -> assert false
    );

    return true
  )

let tests = [
  "basic kkv operations", test_kkv_basic_operations;
  "kkv paging", test_kkv_paging;
]
