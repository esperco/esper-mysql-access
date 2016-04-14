(*
  Arbitrary relation between two sets of keys.

  Each pair of keys (k1, k2) is unique and associated with a string value
  and a numeric value that can be used for ordering.
  Neither column k1 or k2 requires keys
  to be unique in the column.
*)

module type K2V_param =
sig
  val tblname : string
  module Key1 : Mysql_types.Serializable
  module Key2 : Mysql_types.Serializable
  module Value : Mysql_types.Serializable
  module Ord : Mysql_types.Numeric
  val create_ord : Key1.t -> Key2.t -> Value.t -> Ord.t
  val update_ord : (Key1.t -> Key2.t -> Value.t -> Ord.t -> Ord.t) option
end

module type K2V =
sig
  val tblname : string
  type key1
  type key2
  type value
  type ord

  (* Operations on a single element *)

  val get : ?value:value -> key1 -> key2 -> value option Lwt.t
  val get_full : ?value:value -> key1 -> key2 -> (value * ord) option Lwt.t

  val exists : ?value:value -> key1 -> key2 -> bool Lwt.t
  val put : key1 -> key2 -> value -> unit Lwt.t

  val unprotected_put : key1 -> key2 -> value -> ord -> unit Lwt.t
  val delete : ?value:value -> key1 -> key2 -> unit Lwt.t

  val update :
    key1 -> key2 ->
    (value option -> (value option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Set a write lock on the table/key1 pair and on the table/key2 pair,
       read the value and write the new value
       upon termination of the user-given function.
       If the returned value is [None] the original value is preserved.
    *)

  val update_full :
    key1 -> key2 ->
    ((value * ord) option -> (value option * 'a) Lwt.t) ->
    'a Lwt.t
    (* Same as [update] but [ord] is also passed to the callback function *)

  (* Operations on multiple elements *)

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
    ?direction: Mysql_types.direction ->
    ?min_ord: ord ->
    ?max_ord: ord ->
    ?max_count: int ->
    ?value: value ->
    key1 -> (key2 * value * ord) list Lwt.t
    (* return all elements having key1, sorted by 'ord' *)

  val get2 :
    ?direction: Mysql_types.direction ->
    ?min_ord: ord ->
    ?max_ord: ord ->
    ?max_count: int ->
    ?value: value ->
    key2 -> (key1 * value * ord) list Lwt.t
    (* return all elements having key2, sorted by 'ord' *)

  val count1 : ?ord: ord -> ?value: value -> key1 -> int Lwt.t
    (* Count number of entries under key1.
       Also restricted to ord if it is specified. *)

  val delete1 : ?value: value -> key1 -> unit Lwt.t
    (* Set a write lock on the table/key1 pair
       and delete the matching entries. *)

  val delete2 : ?value: value -> key2 -> unit Lwt.t
    (* Set a write lock on the table/key2 pair and delete the entry
       if it exists. *)

  val delete_range :
    ?key1:key1 ->
    ?key2:key2 ->
    ?max_count:int ->
    ?min_ord:ord ->
    ?xmin_ord:ord ->
    ?max_ord:ord ->
    ?xmax_ord:ord ->
    unit -> int64 Lwt.t
    (* Delete multiple rows in a single mysql call,
       return the number of rows deleted. *)

  val lock1 : key1 -> (unit -> 'a Lwt.t) -> 'a Lwt.t
  val lock2 : key2 -> (unit -> 'a Lwt.t) -> 'a Lwt.t
    (* Set a write lock on the table/key pair while the user-given
       function is running.
    *)

  val unprotected_delete1 : ?value: value -> key1 -> unit Lwt.t
    (* Remove the given entries if it exists regardless of the presence
       of a lock. *)

  val unprotected_delete2 : ?value: value -> key2 -> unit Lwt.t
    (* Remove the given entry if it exists regardless of the presence
       of a lock. *)

  (**/**)
  (* testing only; not the authoritative copy for this schema *)
  val create_table : unit -> unit Lwt.t
end

module Make (Param : K2V_param) : K2V
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
  let esc_key1 key = Mysql.escape (Param.Key1.to_string key)
  let esc_key2 key = Mysql.escape (Param.Key2.to_string key)
  let esc_value key = Mysql.escape (Param.Value.to_string key)
  let esc_ord ord =
    Mysql_types.ord_of_float (Param.Ord.to_float ord)

  let ord_of_string s =
    Param.Ord.of_float (float_of_string s)

  let make_where_clause
      ?key1
      ?key2
      ?after
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord
      () =
    let key1_clause =
      match key1 with
      | None -> []
      | Some k1 -> [ sprintf "k1 = '%s'" (esc_key1 k1) ]
    in
    let key2_clause =
      match key2 with
      | None -> []
      | Some k2 -> [ sprintf "k2 = '%s'" (esc_key2 k2) ]
    in
    let after_clause =
      match after with
      | None -> []
      | Some (k1, k2) ->
          let k1_s = esc_key1 k1 in
          let k2_s = esc_key2 k2 in
          [ sprintf "(k1 = '%s' and k2 > '%s' or k1 > '%s')" k1_s k2_s k1_s ]
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
      List.flatten
        [ key1_clause; key2_clause; after_clause;
          min_ord_clause; max_ord_clause ]
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
      sprintf "select k1, k2, v, ord from %s%s order by k1, k2 asc%s;"
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
              ~after:(k1, k2)
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
        () in
    Util_lwt_stream.iter max_threads stream f

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

  let filter_v opt_v =
    match opt_v with
    | None -> ""
    | Some v -> sprintf " and v='%s'" (esc_value v)

  let get_list
      k1_name k1_to_esc
      k2_name k2_of_string
      ?(direction = `Asc) ?min_ord ?max_ord ?max_count ?value k1 =
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
    let st =
      sprintf "select %s, v, ord from %s where %s='%s'%s%s%s%s%s;"
        k2_name
        esc_tblname
        k1_name
        (k1_to_esc k1)
        mini
        maxi
        (filter_v value)
        order
        limit
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res, _affected = Mysql_lwt.unwrap_result x in
      let rows = Mysql_util.fetch_all res in
      BatList.map (function
        | [| Some k2; Some v; Some ord |] ->
            (k2_of_string k2, Param.Value.of_string v, ord_of_string ord)
        |  _ -> failwith ("Broken result returned on: " ^ st)
      ) rows
    )

  let get1 ?direction ?min_ord ?max_ord ?max_count ?value k1 =
    get_list
      "k1" esc_key1
      "k2" Param.Key2.of_string
      ?direction ?min_ord ?max_ord ?max_count ?value k1

  let get2 ?direction ?min_ord ?max_ord ?max_count ?value k2 =
    get_list
      "k2" esc_key2
      "k1" Param.Key1.of_string
      ?direction ?min_ord ?max_ord ?max_count ?value k2

  let get_full ?value k1 k2 =
    let st =
      sprintf "select v, ord from %s where k1='%s' and k2='%s'%s;"
        esc_tblname (esc_key1 k1) (esc_key2 k2) (filter_v value)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let res, _affected = Mysql_lwt.unwrap_result x in
      match Mysql.fetch res with
        | Some [| Some v; Some ord |] ->
            Some (Param.Value.of_string v,
                  Param.Ord.of_float (float_of_string ord))
        | None -> None
        |  _ -> failwith ("Broken result returned on: " ^ st)
    )

  let get ?value k1 k2 =
    get_full ?value k1 k2 >>= function
    | None -> return None
    | Some (v, ord) -> return (Some v)

  let count1
      ?ord
      ?value
      k1 =
    let cond_ord =
      match ord with
      | Some x -> " and ord=" ^ esc_ord x
      | None   -> "" in
    let st =
      sprintf "select count(*) from %s where k1='%s'%s%s;"
        esc_tblname (esc_key1 k1) cond_ord (filter_v value)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      match Mysql.fetch (fst (Mysql_lwt.unwrap_result x)) with
      | Some [| Some n |] -> int_of_string n
      | _ -> failwith ("Broken result returned on: " ^ st)
    )

  let exists ?value k1 k2 =
    get_full ?value k1 k2 >>= function
      | None -> return false
      | Some _ -> return true

  let unprotected_put k1 k2 v ord =
    let st =
      sprintf "\
replace into %s (k1, k2, v, ord) values ('%s', '%s', '%s', %s);
"
        esc_tblname (esc_key1 k1) (esc_key2 k2) (esc_value v) (esc_ord ord)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let delete_range
      ?key1
      ?key2
      ?max_count
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord
      () =
    let where =
      make_where_clause
        ?key1
        ?key2
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

  let delete ?value k1 k2 =
    delete_range ~key1:k1 ~key2:k2 () >>= fun n ->
    return ()

  let unprotected_delete1 ?value key1 =
    let st =
      sprintf "delete from %s where k1='%s'%s;"
        esc_tblname (esc_key1 key1) (filter_v value)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res = Mysql_lwt.unwrap_result x in
      ()
    )

  let unprotected_delete2 ?value key2 =
    let st =
      sprintf "delete from %s where k2='%s'%s;"
        esc_tblname (esc_key2 key2) (filter_v value)
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

  let update_full k1 k2 f =
    lock1 k1 (fun () ->
      lock2 k2 (fun () ->
        get_full k1 k2 >>= fun opt_v_ord ->
        f opt_v_ord >>= fun (opt_v', result) ->
        (match opt_v' with
         | None -> return ()
         | Some v' ->
             let ord' =
               match opt_v_ord with
               | None -> create_ord k1 k2 v'
               | Some (_, ord) -> update_ord k1 k2 v' ord
             in
             unprotected_put k1 k2 v' ord'
        ) >>= fun () ->
        return result
      )
    )

  let update k1 k2 f =
    update_full k1 k2 (function
      | None -> f None
      | Some (v, ord) -> f (Some v)
    )

  let put k1 k2 v =
    update k1 k2 (fun _old ->
      return (Some v, ())
    )

  let delete1 ?value k =
    lock1 k (fun () ->
      unprotected_delete1 ?value k
    )

  let delete2 ?value k =
    lock2 k (fun () ->
      unprotected_delete2 ?value k
    )

  (* NOT the authoritative copy of the schema. Do not use in production. *)
  let create_table () =
    let st =
      sprintf "\
create table if not exists %s (
       k1 varbinary(767) not null,
       k2 varbinary(767) not null,
       v longblob not null,
       ord double not null,

       primary key (k1, k2),
       index (k1),
       index (k2),
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


module Test_param = struct
  let tblname = "testset"
  module Int_key = struct
    type t = int
    let to_string = string_of_int
    let of_string = int_of_string
  end
  module Key1 = Int_key
  module Key2 = Int_key
  module Value = Int_key
  module Ord = struct
    type t = float
    let to_float x = x
    let of_float x = x
  end
  let create_ord k1 k2 v = float k2
  let update_ord = None
end

module Testset = Make (Test_param)

let test () =
  let open Lwt in
  Util_lwt_main.run (
    Testset.create_table () >>= fun () ->
    let l1 = [
      1, 10, 200;
      1, 11, 21;
      1, 12, 22;
      1, 13, 23;
      1, 14, 24;
      2, 13, 230;
    ] in
    Lwt_list.iter_s (fun (k1, k2, v) ->
      Testset.put k1 k2 v
    ) l1 >>= fun () ->

    let normalize_abc_ab l =
      List.sort compare (List.map (fun (a, b, c) -> (a, b)) l)
    in

    let normalize_abc_bc l =
      List.sort compare (List.map (fun (a, b, c) -> (b, c)) l)
    in

    Testset.get1 1 >>= fun l1' ->
    let filtered = List.filter (fun (k1, k2, v) -> k1 = 1) l1 in
    assert (normalize_abc_ab l1' = normalize_abc_bc filtered);

    Testset.get1 ~value:24 1 >>= fun l ->
    assert (normalize_abc_ab l = [14, 24]);

    Testset.get2 12 >>= fun l2 ->
    assert (normalize_abc_ab l2 = [1, 22]);

    Testset.get2 ~value:22 12 >>= fun l2 ->
    assert (normalize_abc_ab l2 = [1, 22]);

    Testset.get2 ~value:23 12 >>= fun l2 ->
    assert (l2 = []);

    Testset.count1 1 >>= fun n ->
    assert (n = List.length l1 - 1);

    Testset.count1 ~value:22 1 >>= fun n ->
    assert (n = 1);

    Testset.to_list ~page_size:2 () >>= fun l ->
    assert (List.length l = 6);

    Testset.to_list ~min_ord:11. ~xmax_ord:13. () >>= fun l ->
    assert (List.map (fun (_, k2, _, _) -> k2) l = [11; 12]);

    Testset.to_list ~xmin_ord:11. ~max_ord:13. () >>= fun l ->
    assert (List.map (fun (_, k2, _, _) -> k2) l = [12; 13; 13]);

    Testset.get2 13 >>= fun l -> assert (l <> []);
    Testset.unprotected_delete2 13 ~value:230 >>= fun () ->
    Testset.get2 13 >>= fun l -> assert (normalize_abc_ab l = [1, 23]);

    Testset.unprotected_delete2 13 >>= fun () ->
    Testset.get2 13 >>= fun l -> assert (l = []);

    Testset.get1 1 >>= fun l -> assert (l <> []);
    Testset.unprotected_delete1 1 ~value:200 >>= fun () ->
    Testset.get1 1 >>= fun l -> assert (l <> []);

    Testset.unprotected_delete1 1 >>= fun () ->
    Testset.get1 1 >>= fun l -> assert (l = []);

    let upd_k1 = 15 and upd_k2 = 25 in
    Testset.update upd_k1 upd_k2 (function
      | None -> return (None, ())
      | Some _ -> assert false
    ) >>= fun () ->

    Testset.update upd_k1 upd_k2 (function
      | None -> return (Some 0, ())
      | Some _ -> assert false
    ) >>= fun () ->

    Testset.update upd_k1 upd_k2 (function
      | Some 0 -> return (Some 1, ())
      | _ -> assert false
    ) >>= fun () ->

    (Testset.get upd_k1 upd_k2 >>= function
     | Some 1 -> return ()
     | _ -> assert false
    ) >>= fun () ->

    Testset.to_list () >>= fun l ->
    let len1 = List.length l in
    Testset.put 1000 2000 3000 >>= fun () ->
    Testset.to_list () >>= fun l ->
    assert (List.length l = len1 + 1);
    Testset.delete_range ~min_ord:2000. ~xmax_ord:2001. () >>= fun n ->
    assert (n = 1L);
    Testset.to_list () >>= fun l ->
    assert (List.length l = len1);

    return true
  )

let tests = [
  "basic list operations", test;
]
