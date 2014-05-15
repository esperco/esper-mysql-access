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
  val remove : key1 -> key2 -> unit Lwt.t

  (* Operations on multiple elements *)

  (* TODO: support proper locking and update *)

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
      let res = Mysql_lwt.unwrap_result x in
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
      let res = Mysql_lwt.unwrap_result x in
      match Mysql.fetch res with
        | Some [| Some v; Some ord |] ->
            Some (Param.Value.of_string v,
                  Param.Ord.of_float (float_of_string ord))
        | None -> None
        |  _ -> failwith ("Broken result returned on: " ^ st)
    )

  let exists k1 k2 =
    get k1 k2 >>= function
      | None -> return false
      | Some _ -> return true

  let put k1 k2 v =
    let ord = Param.create_ord k1 k2 v in
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

  let remove k1 k2 =
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
         -- identifies the set
       k2 varchar(767) character set ascii not null,
         -- identifies an element of the set
         -- (unique within the set but not within the table)
       v longblob not null,
         -- value, often JSON
       ord double not null,
         -- ordering, typically a timestamp

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
    module Ord = Util_time
    let create_ord k1 k2 v = Util_time.now ()
  end
  in
  let module Testset = Make (Param) in
  let open Lwt in
  Lwt_main.run (
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

    return true
  )

let tests = [
  "basic list operations", test;
]
