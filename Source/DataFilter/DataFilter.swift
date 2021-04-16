import Foundation
import CoreData

/**
 Helps you filter insertions, deletions and updates by comparing your JSON dictionary with your Core Data local objects.
 It also provides uniquing for you locally stored objects and automatic removal of not found ones.
 */
class DataFilter: NSObject {
    struct Operation: OptionSet {
        let rawValue: Int

        init(rawValue: Int) {
            self.rawValue = rawValue
        }

        static let insert = Operation(rawValue: 1 << 0)
        static let update = Operation(rawValue: 1 << 1)
        static let delete = Operation(rawValue: 1 << 2)
        static let all: Operation = [.insert, .update, .delete]
    }

    class func changes(_ changes: [[String: Any]],
                       inEntityNamed entityName: String,
                       localPrimaryKey: String,
                       remotePrimaryKey: String,
                       context: NSManagedObjectContext,
                       inserted: (_ json: [String: Any]) -> Void,
                       updated: (_ json: [String: Any], _ updatedObject: NSManagedObject) -> Void) throws {
        try self.changes(changes, inEntityNamed: entityName, predicate: nil, operations: .all, localPrimaryKey: localPrimaryKey, remotePrimaryKey: remotePrimaryKey, context: context, inserted: inserted, updated: updated)
    }

    class func changes(_ changes: [[String: Any]],
                       inEntityNamed entityName: String,
                       predicate: NSPredicate?,
                       operations: Operation,
                       localPrimaryKey: String,
                       remotePrimaryKey: String,
                       context: NSManagedObjectContext,
                       inserted: (_ json: [String: Any]) throws -> Void,
                       updated: (_ json: [String: Any], _ updatedObject: NSManagedObject) throws -> Void) throws {
        // `DataObjectIDs.objectIDsInEntityNamed` also deletes all objects that don't have a primary key or that have the same primary key already found in the context
        let primaryKeysAndObjectIDs = context.managedObjectIDs(in: entityName, usingAsKey: localPrimaryKey, predicate: predicate) as [NSObject: NSManagedObjectID]
        let localPrimaryKeys = Array(primaryKeysAndObjectIDs.keys)
        let remotePrimaryKeys = changes.map { $0[remotePrimaryKey] }
        let remotePrimaryKeysWithoutNils = (remotePrimaryKeys.filter { (($0 as? NSObject) != NSNull()) && ($0 != nil) } as! [NSObject?]) as! [NSObject]

        var remotePrimaryKeysAndChanges = [NSObject: [String: Any]]()
        for (primaryKey, change) in zip(remotePrimaryKeysWithoutNils, changes) {
            remotePrimaryKeysAndChanges[primaryKey] = change
        }

        if operations.contains(.delete) {
            var deletedObjectIDs = localPrimaryKeys
            deletedObjectIDs = deletedObjectIDs.filter { value in
                !remotePrimaryKeysWithoutNils.contains { $0.isEqual(value) }
            }

            for fetchedID in deletedObjectIDs {
                guard let objectID = primaryKeysAndObjectIDs[fetchedID] else {
                    print("error: Failed to get primary key for \(fetchedID)")
                    continue
                }
                let object = context.object(with: objectID)
                context.delete(object)
            }
        }

        if operations.contains(.insert) {
            var insertedObjectIDs = remotePrimaryKeysWithoutNils
            insertedObjectIDs = insertedObjectIDs.filter { value in
                !localPrimaryKeys.contains { $0.isEqual(value) }
            }

            for fetchedID in insertedObjectIDs {
                guard let objectDictionary = remotePrimaryKeysAndChanges[fetchedID] else {
                    print("error: Failed to get primary key for \(fetchedID)")
                    continue
                }
                try inserted(objectDictionary)
            }
        }

        if operations.contains(.update) {
            let intersection = NSMutableOrderedSet(array:remotePrimaryKeysWithoutNils)
            intersection.intersect(NSOrderedSet(array: localPrimaryKeys))
            let updatedObjectIDs = intersection.array

            for case let fetchedID as NSObject in updatedObjectIDs {
                guard let JSON = remotePrimaryKeysAndChanges[fetchedID] else {
                    print("error: Failed to get remote primary key for \(fetchedID)")
                    continue
                }
                guard let objectID = primaryKeysAndObjectIDs[fetchedID] else {
                    print("error: Failed to get primary key for \(fetchedID)")
                    continue
                }
                let object = context.object(with: objectID)
                try updated(JSON, object)
            }
        }
    }
}
