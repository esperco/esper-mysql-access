let fetch_all res =
  let rec aux acc =
    match Mysql.fetch res with
        Some a -> aux (a :: acc)
      | None -> List.rev acc
  in
  aux []
