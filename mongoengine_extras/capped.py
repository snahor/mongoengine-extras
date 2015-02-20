# encoding: utf-8

from time import time
from mongoengine import QuerySet


class CappedQuerySet(QuerySet):
	"""A cusom queryset that allows for tailing of capped collections."""
	
	def tail(self, timeout=None):
		"""A generator which will block and yield entries as they are added to the collection.
		
		Only use this on capped collections; ones with meta containing `max_size` and/or `max_documents`.
		
		Accepts the int/float `timeout` named argument indicating a number of seconds to wait for a result.  This
		value will be an estimate, not a hard limit, until https://jira.mongodb.org/browse/SERVER-15815 is fixed.  It will "snap" to the nearest multiple of the mongod process wait time.
		
		for obj in MyDocument.objects.tail():
			print(obj)
		
		Additional important note: tailing will fail (badly) if the collection is empty.  Always prime the collection
		with an empty or otherwise unimportant record before attempting to use this feature.
		"""
		
		# Process the timeout value, if one is provided.
		if timeout: timeout = float(timeout)
		
		# Prepare the query and extract often-reused values.
		q = self.clone()
		collection = q._collection
		query = q._query
		
		if not collection.options().get('capped', False):
			raise TypeError("Can only operate on capped collections.")
		
		# We track the last seen ID to allow us to efficiently re-query from where we left off.
		last = None
		
		start = time()  # Capture the start time.
		
		while True:
			cursor = collection.find(query, tailable=True, await_data=True, **q._cursor_args)
			
			if timeout:
				start = time()
			
			while True:
				try:
					record = cursor.next()
				except StopIteration:
					if not cursor.alive:
						break
					
					record = None
				
				if timeout:
					end = time()
					
				if record is not None:
					yield self._document._from_son(record, _auto_dereference=self._auto_dereference)
					last = record['_id']
				
				if timeout:
					timeout -= time() - start
					if timeout <= 0:
						return
					start = time()
			
			if last:
				query.update(_id={"$gt": last})
