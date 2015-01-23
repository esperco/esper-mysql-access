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
