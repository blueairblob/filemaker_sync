drop trigger if exists "on_auth_user_created" on "auth"."users";


drop policy "Allow authenticated users to read and upload 16wiy3a_0" on "storage"."objects";

drop policy "Allow authenticated users to read and upload 16wiy3a_1" on "storage"."objects";

drop policy "Anyone can upload an avatar." on "storage"."objects";

drop policy "Avatar images are publicly accessible." on "storage"."objects";


