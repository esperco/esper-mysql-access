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
end

module type K2V =
sig
  val tblname : string
  type key1
  type key2
  type value
  type ord

  (* Operations on a single element *)

  val get : key1 -> key2 -> (value * ord) option Lwt.t
  val exists : key1 -> key2 -> bool Lwt.t
  val put : key1 -> key2 -> value -> unit Lwt.t
  val put_if_new : key1 -> key2 -> value -> bool Lwt.t
  val delete : key1 -> key2 -> unit Lwt.t

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
    key1 -> (key2 * value * ord) list Lwt.t
    (* return all elements having key1, sorted by 'ord' *)

  val get2 :
    ?direction: Mysql_types.direction ->
    ?min_ord: ord ->
    ?max_ord: ord ->
    ?max_count: int ->
    key2 -> (key1 * value * ord) list Lwt.t
    (* return all elements having key2, sorted by 'ord' *)

  val count1 : ?ord: ord -> key1 -> int Lwt.t
    (* Count number of entries under key1.
       Also restricted to ord if it is specified. *)

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

  let esc_tblname = Mysql.escape tblname
  let esc_key1 key = Mysql.escape (Param.Key1.to_string key)
  let esc_key2 key = Mysql.escape (Param.Key2.to_string key)
  let esc_value key = Mysql.escape (Param.Value.to_string key)
  let esc_ord ord =
    Mysql_types.ord_of_float (Param.Ord.to_float ord)

  let ord_of_string s =
    Param.Ord.of_float (float_of_string s)

  type page = (key1 * key2 * value * ord) list

  (* This type definition is purely so that we don't have to
     use the -rectypes compiler option. *)
  type get_page = Get_page of (unit -> (page * get_page) option Lwt.t)

  let rec get_page
      ?after
      ?max_count
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord (): (page * get_page) option Lwt.t =
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
      match page with
      | [] -> None
      | l ->
          let k1, k2, _, _ = BatList.last l in
          Some (page,
                Get_page (fun () ->
                  get_page
                    ~after:(k1, k2)
                    ?max_count
                    ?min_ord
                    ?xmin_ord
                    ?max_ord
                    ?xmax_ord
                    ())
               )
    )

  let to_stream
      ?(page_size = 1000)
      ?min_ord
      ?xmin_ord
      ?max_ord
      ?xmax_ord
      () =
    let q = Queue.create () in
    let get_next_page = ref (
      fun () ->
        get_page
          ~max_count:page_size
          ?min_ord
          ?xmin_ord
          ?max_ord
          ?xmax_ord ()
    ) in
    let rec get_next_item () =
      if Queue.is_empty q then
        (* refill queue *)
        !get_next_page () >>= function
        | None ->
            get_next_page := (fun () -> assert false);
            return None
        | Some (page, Get_page get_next) ->
            assert (page <> []);
            List.iter (fun x -> Queue.add x q) page;
            get_next_page := get_next;
            (* retry read from queue *)
            get_next_item ()
      else
        return (Some (Queue.take q))
    in
    Lwt_stream.from get_next_item

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

  let get_list
      k1_name k1_to_esc
      k2_name k2_of_string
      ?(direction = `Asc) ?min_ord ?max_ord ?max_count k1 =
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
      sprintf "select %s, v, ord from %s where %s='%s'%s%s%s%s;"
        k2_name
        esc_tblname
        k1_name
        (k1_to_esc k1)
        mini
        maxi
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

  let get1 ?direction ?min_ord ?max_ord ?max_count k1 =
    get_list
      "k1" esc_key1
      "k2" Param.Key2.of_string
      ?direction ?min_ord ?max_ord ?max_count k1

  let get2 ?direction ?min_ord ?max_ord ?max_count k2 =
    get_list
      "k2" esc_key2
      "k1" Param.Key1.of_string
      ?direction ?min_ord ?max_ord ?max_count k2

  let get k1 k2 =
    let st =
      sprintf "select v, ord from %s where k1='%s' and k2='%s';"
        esc_tblname (esc_key1 k1) (esc_key2 k2)
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

  let count1
      ?ord
      k1 =
    let cond_ord =
      match ord with
      | Some x -> " and ord=" ^ esc_ord x
      | None   -> "" in
    let st =
      sprintf "select count(*) from %s where k1='%s'%s"
        esc_tblname (esc_key1 k1) cond_ord
    in
    Mysql_lwt.mysql_exec st (fun x ->
      match Mysql.fetch (fst (Mysql_lwt.unwrap_result x)) with
      | Some [| Some n |] -> int_of_string n
      | _ -> failwith ("Broken result returned on: " ^ st)
    )

  let exists k1 k2 =
    get k1 k2 >>= function
      | None -> return false
      | Some _ -> return true

  let put' verb k1 k2 v =
    let ord = Param.create_ord k1 k2 v in
    let st =
      sprintf "\
%s into %s (k1, k2, v, ord) values ('%s', '%s', '%s', %s);
"
        verb esc_tblname (esc_key1 k1) (esc_key2 k2) (esc_value v) (esc_ord ord)
    in
    Mysql_lwt.mysql_exec st (fun x ->
      let _res, affected = Mysql_lwt.unwrap_result x in
      affected
    )

  let put k1 k2 v =
    put' "replace" k1 k2 v >>= fun (_affected:int64) -> return ()

  let put_if_new k1 k2 v =
    put' "insert ignore" k1 k2 v >>= fun affected ->
    return (affected > 0L)

  let delete k1 k2 =
    let st =
      sprintf "delete from %s where k1='%s' and k2='%s';"
        esc_tblname (esc_key1 k1) (esc_key2 k2)
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
       k1 varchar(767) character set ascii not null,
       k2 varchar(767) character set ascii not null,
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


let test () =
  let module Param = struct
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
  end
  in
  let module Testset = Make (Param) in
  let open Lwt in
  Util_lwt_main.run (
    Testset.create_table () >>= fun () ->
    let k1 = 1 in
    let l1 = [
      11, 21;
      12, 22;
      13, 23;
      14, 24;
    ] in
    Lwt_list.iter_s (fun (k2, v) ->
      Testset.put k1 k2 v
    ) l1 >>= fun () ->
    Testset.get1 k1 >>= fun l1' ->
    let normalize l =
      List.sort compare (List.map (fun (k, v, _) -> (k, v)) l)
    in
    assert (normalize l1' = l1);

    Testset.get2 12 >>= fun l2 ->
    assert (normalize l2 = [k1, 22]);

    Testset.count1 1 >>= fun n ->
    assert (n = List.length l1);

    Testset.to_list ~page_size:2 () >>= fun l ->
    assert (List.length l = 4);

    Testset.to_list ~min_ord:11. ~xmax_ord:13. () >>= fun l ->
    Printf.printf "length: %i\n%!" (List.length l);
    assert (List.map (fun (_, k2, _, _) -> k2) l = [11; 12]);

    Testset.to_list ~xmin_ord:11. ~max_ord:13. () >>= fun l ->
    assert (List.map (fun (_, k2, _, _) -> k2) l = [12; 13]);

    return true
  )

let tests = [
  "basic list operations", test;
]
