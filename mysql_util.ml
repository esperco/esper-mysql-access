open Lwt

let fetch_all res =
  let rec aux acc =
    match Mysql.fetch res with
        Some a -> aux (a :: acc)
      | None -> List.rev acc
  in
  aux []

(*
   Order a list of values according to an ordering of the keys.
*)
let reorder key_sequence values get_key =
  let tbl = Hashtbl.create (2 * List.length key_sequence) in
  BatList.iteri (fun i k ->
    if not (Hashtbl.mem tbl k) then
      Hashtbl.add tbl k i
  ) key_sequence;
  let a0 = Array.of_list values in
  let a = Array.map (fun v ->
    let i =
      let k = get_key v in
      try Hashtbl.find tbl k
      with Not_found -> invalid_arg ("Mysql_util.reorder: invalid key " ^ k)
    in
    (i, v)
  ) a0 in
  let cmp (i, _) (j, _) = compare i j in
  Array.fast_sort cmp a;
  Array.to_list (Array.map snd a)

  (* This type definition is purely so that we don't have to
     use the -rectypes compiler option. *)
type 'a get_page = [
  | `Get_page of (unit -> ('a list * 'a get_page) option Lwt.t )
]

let stream_from_pages
    ?(end_on_empty_page = false)
    get_first_page: 'a Lwt_stream.t =
    let q = Queue.create () in
    let get_next_page = ref get_first_page in
    let rec get_next_item () =
      if Queue.is_empty q then
        (* refill queue *)
        !get_next_page () >>= function
        | None ->
            get_next_page := (fun () -> assert false);
            return None
        | Some ([], _) when end_on_empty_page ->
            get_next_page := (fun () -> assert false);
            return None
        | Some (page, `Get_page get_next) ->
            List.iter (fun x -> Queue.add x q) page;
            get_next_page := get_next;
            (* retry read from queue *)
            get_next_item ()
      else
        return (Some (Queue.take q))
    in
    Lwt_stream.from get_next_item

let test_reorder () =
  let keys = [ "once"; "upon"; "a"; "time"; "once" ] in
  let values = [
    (0, "upon");
    (0, "time");
    (0, "once");
    (0, "upon");
  ] in
  let get_key (_, s) = s in
  let expected = [
    (0, "once");
    (0, "upon");
    (0, "upon");
    (0, "time");
  ] in
  let result = reorder keys values get_key in
  result = expected

let test_stream () =
  let rec get_page last =
    let x1 = last + 1 in
    let x2 = x1 + 1 in
    if x2 <= 4 then
      let page = [ x1; x2 ] in
      let get_page () = get_page x2 in
      return (Some (page, `Get_page get_page))
    else
      return None
  in
  let stream = stream_from_pages (fun () -> get_page 0) in
  let result = Lwt_main.run (Lwt_stream.to_list stream) in
  result = [ 1; 2; 3; 4; ]

let tests = [
  "reorder", test_reorder;
  "stream", test_stream;
]
