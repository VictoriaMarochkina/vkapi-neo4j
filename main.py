import requests
import logging
from neo4j import GraphDatabase

VK_TOKEN = ""
VK_API_URL = "https://api.vk.com/method/"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def close_driver():
    driver.close()


def run_queries():
    while True:
        query_type = input(
            "Выберите запрос (1 - всего пользователей, 2 - всего групп, 3 - топ пользователей, 4 - топ групп, "
            "5 - топ по подпискам, 0 - выйти): ")

        if query_type == "0":
            print("Завершение программы.")
            break

        with driver.session() as session:
            if query_type == "1":
                result = session.run("MATCH (u:User) RETURN count(u) AS total_users")
                print(f"Total users: {result.single()['total_users']}")

            elif query_type == "2":
                result = session.run("MATCH (g:Group) RETURN count(g) AS total_groups")
                print(f"Total groups: {result.single()['total_groups']}")

            elif query_type == "3":
                limit = int(input("Введите лимит для топа пользователей по количеству фолловеров: "))
                result = session.run("""
                    MATCH (u:User)<-[:Follow]-(follower:User)
                    RETURN u.id AS user_id, u.name AS user_name, count(follower) AS followers_count
                    ORDER BY followers_count DESC
                    LIMIT $limit
                """, limit=limit)
                print("Top users by followers:")
                for record in result:
                    print(
                        f"User ID: {record['user_id']}, Name: {record['user_name']}, Followers: {record['followers_count']}")

            elif query_type == "4":
                limit = int(input("Введите лимит для топа групп по количеству подписчиков: "))
                result = session.run("""
                    MATCH (g:Group)
                    RETURN g.id AS group_id, g.name AS group_name, g.subscribers_count AS subscribers_count
                    ORDER BY subscribers_count DESC
                    LIMIT $limit
                """, limit=limit)
                print("Top groups by subscribers:")
                for record in result:
                    print(
                        f"Group ID: {record['group_id']}, Name: {record['group_name']}, Subscribers: {record['subscribers_count']}")

            elif query_type == "5":
                limit = int(input("Введите лимит для топа пользователей по подпискам на группы: "))
                result = session.run("""
                    MATCH (u:User)-[:Subscribe]->(g:Group)
                    RETURN u.id AS user_id, u.name AS user_name, COUNT(g) AS group_subscriptions
                    ORDER BY group_subscriptions DESC
                    LIMIT $limit
                """, limit=limit)
                print("Top users by group subscriptions:")
                for record in result:
                    print(
                        f"User ID: {record['user_id']}, Name: {record['user_name']}, Subscriptions: {record['group_subscriptions']}")

            else:
                print("Неверный выбор запроса. Попробуйте снова.")


def get_user_data(user_id):
    url = VK_API_URL + "users.get"
    params = {
        "user_ids": user_id,
        "fields": "first_name,last_name,sex,home_town,city",
        "access_token": VK_TOKEN,
        "v": "5.131"
    }
    response = requests.get(url, params=params)
    return response.json()


def get_followers(user_id):
    followers = []
    offset = 0
    count = 100

    url = VK_API_URL + "users.getFollowers"
    params = {
        "user_id": user_id,
        "count": 1,
        "access_token": VK_TOKEN,
        "v": "5.131"
    }
    response = requests.get(url, params=params).json()
    total_followers = response.get('response', {}).get('count', 0)

    if total_followers > 300:
        logger.info(f"Пропускаем пользователя {user_id}, так как у него более 500 подписчиков.")
        return []

    while offset < total_followers:
        params = {
            "user_id": user_id,
            "count": count,
            "offset": offset,
            "access_token": VK_TOKEN,
            "v": "5.131"
        }
        response = requests.get(url, params=params).json()
        items = response.get('response', {}).get('items', [])
        followers.extend(items)

        if not items:
            break

        offset += count

    return followers


def get_followers_info(follower_ids):
    url = VK_API_URL + "users.get"
    params = {
        "user_ids": ",".join(map(str, follower_ids)),
        "fields": "first_name,last_name",
        "access_token": VK_TOKEN,
        "v": "5.131"
    }
    response = requests.get(url, params=params)
    return response.json()


def get_subscriptions(user_id):
    url = VK_API_URL + "users.getSubscriptions"
    params = {
        "user_id": user_id,
        "extended": 1,
        "access_token": VK_TOKEN,
        "v": "5.131"
    }
    response = requests.get(url, params=params)
    return response.json()


def get_groups_info(group_ids):
    url = VK_API_URL + "groups.getById"
    params = {
        "group_ids": ",".join(map(str, group_ids)),
        "fields": "name,members_count",
        "access_token": VK_TOKEN,
        "v": "5.131"
    }
    response = requests.get(url, params=params)
    return response.json()


def save_user(tx, user):
    city = user.get('city', {}).get('title', '')
    home_town = user.get('home_town', '') or city

    tx.run(
        """
        MERGE (u:User {id: $id})
        SET u.screen_name = $screen_name,
            u.name = $name,
            u.sex = $sex,
            u.home_town = $home_town
        """,
        id=user['id'],
        screen_name=user.get('screen_name', ''),
        name=f"{user.get('first_name', '')} {user.get('last_name', '')}",
        sex=user.get('sex', ''),
        home_town=home_town
    )


def save_group(tx, group):
    tx.run(
        """
        MERGE (g:Group {id: $id})
        SET g.name = $name, 
            g.screen_name = $screen_name,
            g.subscribers_count = $members_count
        """,
        id=group['id'],
        name=group.get('name', ''),
        screen_name=group.get('screen_name', ''),
        members_count=group.get('members_count', 0)
    )


def create_relationship(tx, user_id, target_id, rel_type):
    tx.run(
        f"""
        MATCH (u:User {{id: $user_id}})
        MATCH (target {{id: $target_id}})
        MERGE (u)-[:{rel_type}]->(target)
        """,
        user_id=user_id, target_id=target_id
    )
    logger.info(f"Связь {rel_type} создана между {user_id} и {target_id}")


def process_user(user_id, level, max_level):
    queue = [(user_id, level)]
    visited = set()

    while queue:
        current_id, current_level = queue.pop(0)

        if current_id in visited or current_level > max_level:
            continue
        visited.add(current_id)

        user_data = get_user_data(current_id)
        if user_data is None or 'response' not in user_data:
            logger.warning(f"Не удалось получить данные для пользователя {current_id}")
            continue
        user_info = user_data['response'][0]

        with driver.session() as session:
            session.execute_write(save_user, user_info)
            logger.info(f"Добавлен пользователь {user_info['id']} на уровне {current_level}")

            followers_data = get_followers(current_id)
            if followers_data:
                logger.info(f"Найдено {len(followers_data)} фолловеров для пользователя {current_id}")
                follower_ids = followers_data
                followers_info = get_followers_info(follower_ids)
                for follower in followers_info.get('response', []):
                    session.execute_write(save_user, follower)
                    session.execute_write(create_relationship, follower['id'], current_id, "Follow")
                    queue.append((follower['id'], current_level + 1))
                    logger.info(f"Добавлен фолловер {follower['id']} для пользователя {current_id} на уровне {current_level + 1}")
            else:
                logger.info(f"Нет фолловеров для пользователя {current_id}")

            subscriptions_data = get_subscriptions(current_id)
            if 'response' in subscriptions_data and 'items' in subscriptions_data['response']:
                user_group_ids = [sub['id'] for sub in subscriptions_data['response']['items'] if sub.get('type') == 'group']
                if user_group_ids:
                    logger.info(f"Найдено {len(user_group_ids)} подписок на группы для пользователя {current_id}")
                    groups_info = get_groups_info(user_group_ids)
                    for group in groups_info.get('response', []):
                        session.execute_write(save_group, group)
                        session.execute_write(create_relationship, current_id, group['id'], "Subscribe")
                        logger.info(f"Добавлена подписка на группу {group['id']} для пользователя {current_id}")
                else:
                    logger.info(f"Нет подписок на группы для пользователя {current_id}")
            else:
                logger.info(f"Нет данных о подписках для пользователя {current_id}")

        logger.info(f"Уровень {current_level} обработан для пользователя {current_id}. Переход на уровень {current_level + 1}.\n")

    logger.info("Обработка фолловеров и подписок завершена.")


def main():
    if not VK_TOKEN:
        logger.error("Токен VK API не задан")
        return

    user_id_input = input("Введите ID пользователя или его screen_name: ")
    user_data = get_user_data(user_id_input)

    if user_data and 'response' in user_data:
        user_info = user_data['response'][0]
        user_id = user_info['id']
        max_level = 2
        process_user(user_id, 0, max_level)
    else:
        logger.error("Не удалось получить данные пользователя")

    run_queries()
    close_driver()


if __name__ == "__main__":
    main()