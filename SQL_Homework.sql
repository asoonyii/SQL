USE sakila;
-- No 1a. Display the first and last names of all actors from the table `actor`. 
SELECT first_name AS 'First Name', last_name AS 'Last Name'
FROM actor;

-- No 1b. Display the first and last name of each actor in a single column in upper case
--  letters. Name the column `Actor Name`. 
SELECT upper(concat(first_name, ' ', last_name)) AS "Actor Name"
FROM actor;

-- No 2a.  You need to find the ID number, first name, and last name of an actor, of whom you
-- know only the first name, "Joe." What is  one query would you use to obtain this information?
SELECT actor_id AS "ID number", first_name AS 'First Name', last_name AS 'Last Name'
FROM actor
WHERE first_name LIKE "Joe";

-- No 2b.  Find all actors whose last name contain the letters `GEN`
SELECT first_name AS 'First Name', last_name AS 'Last Name'
FROM actor
WHERE last_name LIKE "%GEN%";

-- No 2c. Find all actors whose last names contain the letters `LI`.  This time, order the rows
-- by last name and first name, in that order:
SELECT  last_name AS 'Last Name', first_name AS 'First Name'
FROM actor
WHERE last_name LIKE "%LI%";

-- No 2d. Using `IN`, display the `country_id` and `country` columns of the following countires:
--  Afghanistan, Bangladesh, and China
SELECT * FROM country
WHERE country IN ('Afghanistan', 'Bangladesh', 'China');


-- No 3a. You want to keep a description of each actor. You don't think you will be 
-- performing queries on a description, so create a column in the table actor named 
-- description and use the data type BLOB (Make sure to research the type BLOB, as 
-- the difference between it and VARCHAR are significant).
ALTER TABLE actor
ADD COLUMN Description BLOB;

-- No 3b. Very quickly you realize that entering descriptions for each actor is too much 
-- effort. Delete the description column.
ALTER TABLE actor
DROP COLUMN Description;

-- No 4a. List the last names of actors, as well as how many actors have that last name.
SELECT last_name, COUNT(last_name) AS 'Number of Actors' 
FROM actor GROUP BY last_name;

-- No 4b. List last names of actors and the number of actors who have that last name, 
-- but only for names that are shared by at least two actors
SELECT last_name, COUNT(last_name) AS 'Number of Actors' 
FROM actor GROUP BY last_name
HAVING COUNT(last_name) >= 2 ;

-- No 4c. The actor HARPO WILLIAMS was accidentally entered in the actor table as 
-- GROUCHO WILLIAMS. Write a query to fix the record.
UPDATE actor 
SET first_name = 'HARPO'
WHERE First_name = "Groucho" AND last_name = "Williams";

-- No 4d. Perhaps we were too hasty in changing GROUCHO to HARPO. It turns out that 
-- GROUCHO was the correct name after all! In a single query, if the first name of 
-- the actor is currently HARPO, change it to GROUCHO.
SELECT actor_id
FROM actor
WHERE first_name= "Harpo";

UPDATE actor 
SET first_name = 'GROUCHO'
WHERE actor_id =172;

-- No 5a. You cannot locate the schema of the address table. Which query would 
-- you use to re-create it?
DESCRIBE sakila.address;

-- No 6a. Use JOIN to display the first and last names, as well as the address, 
-- of each staff member. Use the tables staff and address:
SELECT first_name, last_name, address
FROM staff s 
JOIN address a
ON a.address_id = s.address_id;

-- No 6b. Use JOIN to display the total amount rung up by each staff member in 
-- August of 2005. Use tables staff and payment.
SELECT p.staff_id, s.first_name, s.last_name, sum(p.amount) as "Total Amount"
FROM staff s
INNER JOIN payment p 
ON p.staff_id = s.staff_id AND payment_date LIKE '2005-08%'
GROUP BY p.staff_id; 

-- No 6c. List each film and the number of actors who are listed for that film.
-- Use tables film_actor and film. Use inner join.
SELECT f.title AS 'Film Title', COUNT(ac.actor_id) AS `Number of Actors`
FROM film_actor ac
INNER JOIN film f 
ON f.film_id= ac.film_id
GROUP BY f.title;

-- 6d. How many copies of the film Hunchback Impossible exist in the inventory system?
SELECT title, COUNT(inventory_id)
FROM film f
INNER JOIN inventory i 
ON f.film_id = i.film_id
WHERE title = "Hunchback Impossible";

-- 6e. Using the tables payment and customer and the JOIN command, list the total 
-- paid by each customer. List the customers alphabetically by last name:
SELECT first_name, last_name, SUM(amount) AS "Total Amount"
FROM payment p
INNER JOIN customer c
ON c.customer_id = p.customer_id
GROUP BY p.customer_id
ORDER BY last_name ASC;

-- 7a. The music of Queen and Kris Kristofferson have seen an unlikely resurgence. 
-- As an unintended consequence, films starting with the letters K and Q have also 
-- soared in popularity. Use subqueries to display the titles of movies starting with the l
-- etters K and Q whose language is English.
SELECT title 
FROM film
WHERE language_id in
								(SELECT language_id 
								FROM language
								WHERE name = "English" )
AND (title LIKE "K%") OR (title LIKE "Q%");

-- No 7b. Use subqueries to display all actors who appear in the film Alone Trip.
SELECT last_name, first_name
FROM actor
WHERE actor_id in
					(SELECT actor_id 
					FROM film_actor
					WHERE film_id in 
						(SELECT film_id FROM film
						WHERE title = "Alone Trip"));

-- No 7c. You want to run an email marketing campaign in Canada, for which you 
-- will need the names and email addresses of all Canadian customers. Use joins 
-- to retrieve this information.
SELECT country, last_name, first_name, email
FROM country co
LEFT JOIN customer cu
ON co.country_id = cu.customer_id
WHERE country = 'Canada';

-- No 7d. Sales have been lagging among young families, and you wish to target 
-- all family movies for a promotion. Identify all movies categorized as family films.
SELECT title, category
FROM film_list
WHERE category = 'Family';

-- No 7e. Display the most frequently rented movies in descending order.
SELECT i.film_id, f.title, COUNT(r.inventory_id) AS "Times Rented"
FROM inventory i
INNER JOIN rental r
ON i.inventory_id = r.inventory_id
INNER JOIN film_text f 
ON i.film_id = f.film_id
GROUP BY r.inventory_id
ORDER BY COUNT(r.inventory_id) DESC;

-- No 7f. Write a query to display how much business, in dollars, each store brought in.
SELECT x.store_id, SUM(amount) as "Total Amount"
FROM store x
INNER JOIN staff y
ON x.store_id = y.store_id
INNER JOIN payment p 
ON p.staff_id = y.staff_id
GROUP BY x.store_id;

-- No 7g. Write a query to display for each store its store ID, city, and country.
SELECT s.store_id, city, country
FROM store s
INNER JOIN customer cu
ON s.store_id = cu.store_id
INNER JOIN staff st
ON s.store_id = st.store_id
INNER JOIN address a
ON cu.address_id = a.address_id
INNER JOIN city ci
ON a.city_id = ci.city_id
INNER JOIN country co
ON ci.country_id = co.country_id;

-- No 7h. List the top five genres in gross revenue in descending order. 
-- (Hint: you may need to use the following tables: category, film_category, 
-- inventory, payment, and rental.)

SELECT c.name, SUM(p.amount) AS "Gross Revenue"
FROM category AS c
INNER JOIN film_category AS fc
    ON c.category_id = fc.category_id
INNER JOIN inventory AS i
    ON fc.film_id = i.film_id
INNER JOIN rental AS r
    ON i.inventory_id = r.inventory_id
INNER JOIN payment AS p
    ON r.rental_id = p.rental_id
GROUP BY name
ORDER BY "Gross Revenue" DESC
LIMIT 5;

-- No 8a. In your new role as an executive, you would like to have an easy way 
-- of viewing the Top five genres by gross revenue. Use the solution from the 
-- problem above to create a view. If you haven't solved 7h, you can substitute 
-- another query to create a view.
CREATE VIEW Top_Five_Genres AS
SELECT c.name, SUM(p.amount) AS "Gross Revenue"
FROM category AS c
INNER JOIN film_category AS fc
    ON c.category_id = fc.category_id
INNER JOIN inventory AS i
    ON fc.film_id = i.film_id
INNER JOIN rental AS r
    ON i.inventory_id = r.inventory_id
INNER JOIN payment AS p
    ON r.rental_id = p.rental_id
GROUP BY name
ORDER BY "Gross Revenue" DESC
LIMIT 5;

-- No 8b. How would you display the view that you created in 8a?
SELECT * FROM Top_Five_Genres;

-- No 8c. You find that you no longer need the view top_five_genres. 
-- Write a query to delete it.
DROP VIEW Top_Five_Genres;

