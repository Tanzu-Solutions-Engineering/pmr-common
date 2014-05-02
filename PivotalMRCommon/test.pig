SET pig.splitCombination false;

A = LOAD 'hawq://phd5:5432/ajshook' USING com.gopivotal.pig.HawqLoader('retail_demo.products_dim', 'ajshook');
B = LOAD 'hawq://phd5:5432/ajshook' USING com.gopivotal.pig.HawqLoader('retail_demo.vinyl_counts2', 'ajshook');
C = JOIN A BY category_id, B by category_id USING 'replicated';
DESCRIBE A;
DESCRIBE B;
STORE C INTO '$output';
