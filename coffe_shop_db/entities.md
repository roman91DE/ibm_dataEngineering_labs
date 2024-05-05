# Entities

## Staff Member

## Sale Outlet

## Transaction (Sale)

* transaction_id (primary key)
* timestamp
* sale_outlet_id (foreign key, references sale_outlet)
* staff_id (foreign key, references staff_member)
* customer_id (foreign key, references customer)
* product_id (foreign key, references product)
* quantity
* price

## Customer

## Product

