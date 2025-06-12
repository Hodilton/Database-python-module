import asyncio
from log_manager import LogManager
from data_base import Database, DatabaseError

async def run():
    try:
        async with Database() as db:
            # ---------- GET REPO ----------
            users_repo = db.get_repository("users")

            # ---------- CREATE ----------
            await users_repo.create()

            # ---------- INSERT ----------
            await users_repo.insert(("alice", "alice@example.com", "hashed_pw"))
            await users_repo.insert(("bob", "bob@example.com", "hashed_pw2"))

            # ---------- FETCH ONE by username ----------
            user = await users_repo.fetch_one("by_username", ("alice",))
            print("Fetched by username:", user)

            # ---------- FETCH ONE by id ----------
            user_id = user["id"] if user else None
            user_by_id = await users_repo.fetch_one("by_id", (user_id,))
            print("Fetched by ID:", user_by_id)

            # ---------- FETCH ALL ----------
            all_users = await users_repo.fetch_many("all")
            print("All users:", all_users)

            # ---------- FETCH ALL EXCEPT ----------
            all_except = await users_repo.fetch_many("all_except", (user_id,))
            print("All except ID:", all_except)

            # ---------- FETCH COUNT ----------
            count_result = await users_repo.fetch_one("count")
            count = list(count_result.values())[0] if count_result else 0
            print("Total users count:", count)

            # ---------- UPDATE ----------
            await users_repo.update(("alice_updated", "alice_new@example.com", "new_hashed_pw"), (user_id,))
            updated_user = await users_repo.fetch_one("by_id", (user_id,))
            print("Updated user:", updated_user)

            # ---------- DELETE ----------
            await users_repo.delete("by_id", (user_id,))
            after_delete = await users_repo.fetch_many("all")
            print("All users after delete:", after_delete)

            # ---------- DROP ----------
            await users_repo.drop()
    except DatabaseError as e:
        print(f"Error: {e}")
    finally:
        await Database.close_instance()

if __name__ == "__main__":
    log_manager = LogManager("config/logging_config.json")
    log_manager.setup_logging()

    asyncio.run(run())
